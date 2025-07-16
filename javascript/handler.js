import PWRJS from "@pwrjs/core";
import fetch from 'node-fetch';
import DatabaseService from './databaseService.js';

const VIDA_ID = 73746238;
const RPC_URL = "https://pwrrpc.pwrlabs.io/";
const REQUEST_TIMEOUT = 10000;

export let peersToCheckRootHashWith = [];
let pwrjsClient = null;
let subscription = null;

// Fetches the root hash from a peer node for the specified block number
async function fetchPeerRootHash(peer, blockNumber) {
    const url = `http://${peer}/rootHash?blockNumber=${blockNumber}`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            timeout: REQUEST_TIMEOUT,
            headers: {
                'Accept': 'text/plain'
            }
        });
        
        if (response.ok) {
            const hexString = await response.text();
            const trimmed = hexString.trim();
            
            if (!trimmed) {
                console.log(`Peer ${peer} returned empty root hash for block ${blockNumber}`);
                return { success: false, rootHash: null };
            }
            
            try {
                const rootHash = Buffer.from(trimmed, 'hex');
                console.log(`Successfully fetched root hash from peer ${peer} for block ${blockNumber}`);
                return { success: true, rootHash };
            } catch (error) {
                console.log(`Invalid hex response from peer ${peer} for block ${blockNumber}`);
                return { success: false, rootHash: null };
            }
        } else {
            console.log(`Peer ${peer} returned HTTP ${response.status} for block ${blockNumber}`);
            return { success: true, rootHash: null };
        }
    } catch (error) {
        console.log(`Failed to fetch root hash from peer ${peer} for block ${blockNumber}`);
        return { success: false, rootHash: null };
    }
}

// Validates the local Merkle root against peers and persists it if a quorum of peers agree
async function checkRootHashValidityAndSave(blockNumber) {
    const localRoot = await DatabaseService.getRootHash();
    
    if (!localRoot) {
        console.log(`No local root hash available for block ${blockNumber}`);
        return;
    }
    
    let peersCount = peersToCheckRootHashWith.length;
    let quorum = Math.floor((peersCount * 2) / 3) + 1;
    let matches = 0;
    
    for (const peer of peersToCheckRootHashWith) {
        const { success, rootHash } = await fetchPeerRootHash(peer, blockNumber);
        
        if (success && rootHash) {
            if (localRoot.equals(rootHash)) {
                matches++;
            }
        } else {
            if (peersCount > 0) {
                peersCount--;
                quorum = Math.floor((peersCount * 2) / 3) + 1;
            }
        }
        
        if (matches >= quorum) {
            await DatabaseService.setBlockRootHash(blockNumber, localRoot);
            console.log(`Root hash validated and saved for block ${blockNumber}`);
            return;
        }
    }
    
    console.log(`Root hash mismatch: only ${matches}/${peersToCheckRootHashWith.length} peers agreed`);
    await DatabaseService.revertUnsavedChanges();
    subscription.setLatestCheckedBlock(BigInt(await DatabaseService.getLastCheckedBlock()));
}

// Executes a token transfer described by the given JSON payload
async function handleTransfer(jsonData, senderHex) {
    const amount = BigInt(jsonData.amount || 0);
    const receiverHex = jsonData.receiver || "";
    
    if (amount <= 0 || !receiverHex) {
        console.log("Skipping invalid transfer:", jsonData);
        return;
    }

    const senderAddress = senderHex.startsWith("0x") ? senderHex.slice(2) : senderHex;
    const receiverAddress = receiverHex.startsWith("0x") ? receiverHex.slice(2) : receiverHex;
    
    const sender = Buffer.from(senderAddress, 'hex');
    const receiver = Buffer.from(receiverAddress, 'hex');
    
    const success = await DatabaseService.transfer(sender, receiver, amount);
    
    if (success) {
        console.log(`Transfer succeeded: ${amount} from ${senderHex} to ${receiverHex}`);
    } else {
        console.log(`Transfer failed (insufficient funds): ${amount} from ${senderHex} to ${receiverHex}`);
    }
}

// Processes a single VIDA transaction
function processTransaction(txn) {    
    try {
        const dataBytes = Buffer.from(txn.data, 'hex');
    
        const dataStr = dataBytes.toString('utf8');
        const jsonData = JSON.parse(dataStr);
        
        const action = jsonData.action || "";
        
        if (action.toLowerCase() === "transfer") {
            handleTransfer(jsonData, txn.sender);
        }
    } catch (error) {
        console.error("Error processing transaction:", txn.hash, error);
    }
}

// Callback invoked as blocks are processed
async function onChainProgress(blockNumber) {
    try {
        await DatabaseService.setLastCheckedBlock(blockNumber);
        await checkRootHashValidityAndSave(blockNumber);
        console.log(`Checkpoint updated to block ${blockNumber}`);
        await DatabaseService.flush();
    } catch (error) {
        console.error("Failed to update last checked block:", blockNumber, error);
    } finally {
        return null;
    }
}

// Subscribes to VIDA transactions starting from the given block
export async function subscribeAndSync(fromBlock) {
    console.log(`Starting VIDA transaction subscription from block ${fromBlock}`);

    // Initialize RPC client
    pwrjsClient = new PWRJS(RPC_URL);
    
    // Subscribe to VIDA transactions
    subscription = pwrjsClient.subscribeToVidaTransactions(
        VIDA_ID,
        BigInt(fromBlock),
        processTransaction,
        onChainProgress
    );
    console.log(`Successfully subscribed to VIDA ${VIDA_ID} transactions`);
}
