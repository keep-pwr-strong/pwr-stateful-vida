import PWRJS from "@pwrjs/core";
import express from 'express';
import fetch from 'node-fetch';
import { GET } from './api/get.js';
import DatabaseService from './databaseService.js';

const VIDA_ID = 73746238;
const START_BLOCK = 1;
const RPC_URL = "https://pwrrpc.pwrlabs.io/";
const PORT = 8080;
const REQUEST_TIMEOUT = 10000;

const INITIAL_BALANCES = new Map([
    [Buffer.from("c767ea1d613eefe0ce1610b18cb047881bafb829", 'hex'), 1000000000000n],
    [Buffer.from("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4", 'hex'), 1000000000000n],
    [Buffer.from("9282d39ca205806473f4fde5bac48ca6dfb9d300", 'hex'), 1000000000000n],
    [Buffer.from("e68191b7913e72e6f1759531fbfaa089ff02308a", 'hex'), 1000000000000n],
]);

let pwrjsClient = null;
let peersToCheckRootHashWith = [];
let app = null;
let subscription = null;
let blockProgressMonitor = null;

async function main() {
    console.log("Starting PWR VIDA Transaction Synchronizer...");
    
    initializePeers();
    await DatabaseService.initialize();
    await startApiServer();
    await initInitialBalances();
    
    const lastBlock = await DatabaseService.getLastCheckedBlock();
    const fromBlock = lastBlock > 0 ? lastBlock : START_BLOCK;
    
    console.log(`Starting synchronization from block ${fromBlock}`);
    
    await subscribeAndSync(fromBlock);
    
    console.log("Application started successfully. Press Ctrl+C to exit.");
    
    setupShutdownHandlers();
    
    await keepAlive();
}

function initializePeers() {
    const args = process.argv.slice(2);
    
    if (args.length > 0) {
        peersToCheckRootHashWith = args;
        console.log("Using peers from args:", peersToCheckRootHashWith);
    } else {
        peersToCheckRootHashWith = [
            "localhost:8080"
        ];
        console.log("Using default peers:", peersToCheckRootHashWith);
    }
}

async function startApiServer() {
    app = express();
    
    GET.run(app);
    
    return new Promise((resolve, reject) => {
        const server = app.listen(PORT, '0.0.0.0', (err) => {
            if (err) {
                reject(err);
            } else {
                console.log(`Starting API server on port ${PORT}`);
                setTimeout(() => {
                    console.log(`API server started on http://0.0.0.0:${PORT}`);
                    resolve(server);
                }, 2000);
            }
        });
    });
}

async function initInitialBalances() {
    const lastCheckedBlock = await DatabaseService.getLastCheckedBlock();
    
    if (lastCheckedBlock === 0) {
        console.log("Setting up initial balances for fresh database");
        
        for (const [address, balance] of INITIAL_BALANCES) {
            await DatabaseService.setBalance(address, balance);
            console.log(`Set initial balance for ${address.toString('hex')}: ${balance}`);
        }
        
        await DatabaseService.flush();
        console.log("Initial balances setup completed");
    }
}

async function subscribeAndSync(fromBlock) {
    console.log(`Starting VIDA transaction subscription from block ${fromBlock}`);
    
    pwrjsClient = new PWRJS(RPC_URL);
    
    subscription = pwrjsClient.subscribeToVidaTransactions(
        VIDA_ID,
        BigInt(fromBlock),
        processTransaction
    );
    
    console.log(`Successfully subscribed to VIDA ${VIDA_ID} transactions`);
    
    startBlockProgressMonitor(fromBlock);
    
    console.log("Block progress monitor started");
}

function startBlockProgressMonitor(startBlock) {
    let lastChecked = startBlock;
    
    blockProgressMonitor = setInterval(async () => {
        try {
            const latestChainBlock = await getLatestChainBlock();
            
            if (lastChecked < latestChainBlock) {
                const nextBlock = Math.min(lastChecked + 1000, latestChainBlock);
                
                if (nextBlock > lastChecked) {
                    try {
                        await onChainProgress(nextBlock);
                        lastChecked = nextBlock;
                    } catch (error) {
                        console.error("Error in chain progress:", error);
                    }
                }
            }
        } catch (error) {
            console.error("Error in block progress monitor:", error);
        }
    }, 5000);
}

async function getLatestChainBlock() {
    try {
        const latestBlock = await pwrjsClient.getLatestBlockNumber();
        return Number(latestBlock);
    } catch (error) {
        console.error("Failed to get latest chain block:", error);
        return await DatabaseService.getLastCheckedBlock();
    }
}

async function onChainProgress(blockNumber) {
    await DatabaseService.setLastCheckedBlock(blockNumber);
    await checkRootHashValidityAndSave(blockNumber);
    console.log(`Checkpoint updated to block ${blockNumber}`);
    
    await DatabaseService.flush();
}

function processTransaction(txn) {
    console.log(`TRANSACTION RECEIVED: ${txn.data}`);
    
    try {
        handleTransaction(txn);
    } catch (error) {
        console.error("Error processing transaction:", error);
    }
}

function handleTransaction(txn) {
    const dataBytes = Buffer.from(txn.data, 'hex');
    
    const dataStr = dataBytes.toString('utf8');
    const jsonData = JSON.parse(dataStr);
    
    const action = jsonData.action || "";
    
    if (action.toLowerCase() === "transfer") {
        handleTransfer(jsonData, txn.sender);
    }
}

async function handleTransfer(jsonData, senderHex) {
    const amount = BigInt(jsonData.amount || 0);
    const receiverHex = jsonData.receiver || "";
    
    if (amount <= 0 || !receiverHex) {
        console.log("Invalid transfer data:", jsonData);
        return;
    }
    
    const sender = decodeHexAddress(senderHex);
    const receiver = decodeHexAddress(receiverHex);
    
    const success = await DatabaseService.transfer(sender, receiver, amount);
    
    if (success) {
        console.log(`Transfer succeeded: ${amount} from ${senderHex} to ${receiverHex}`);
    } else {
        console.log(`Transfer failed (insufficient funds): ${amount} from ${senderHex} to ${receiverHex}`);
    }
}

function decodeHexAddress(hexStr) {
    const cleanHex = hexStr.startsWith("0x") ? hexStr.slice(2) : hexStr;
    return Buffer.from(cleanHex, 'hex');
}

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
}

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

function setupShutdownHandlers() {
    const gracefulShutdown = async (signal) => {
        console.log(`Received ${signal}, shutting down gracefully...`);
        await shutdown();
        process.exit(0);
    };
    
    process.on('SIGINT', gracefulShutdown);
    process.on('SIGTERM', gracefulShutdown);
}

async function keepAlive() {
    return new Promise(() => {});
}

async function shutdown() {
    console.log("Shutting down application...");
    
    try {
        if (blockProgressMonitor) {
            clearInterval(blockProgressMonitor);
        }
        
        if (subscription) {
            subscription.stop();
        }
        
        await DatabaseService.flush();
        console.log("Flushed database changes");
        
        await DatabaseService.close();
        console.log("Closed database service");
    } catch (error) {
        console.error("Error during shutdown:", error);
    }
    
    console.log("Application shutdown complete");
}

main().catch(console.error);
