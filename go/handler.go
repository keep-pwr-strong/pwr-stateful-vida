package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"pwr-stateful-vida/dbservice"
	"github.com/pwrlabs/pwrgo/rpc"
)

var subscription *rpc.VidaTransactionSubscription

// fetchPeerRootHash fetches the root hash from a peer node for the specified block number
func fetchPeerRootHash(peer string, blockNumber int64) (bool, []byte) {
	url := fmt.Sprintf("http://%s/rootHash?blockNumber=%d", peer, blockNumber)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("Failed to fetch root hash from peer %s for block %d\n", peer, blockNumber)
		return false, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := io.ReadAll(resp.Body)
		hexString := strings.TrimSpace(string(body))

		if hexString == "" {
			fmt.Printf("Peer %s returned empty root hash for block %d\n", peer, blockNumber)
			return false, nil
		}

		rootHash, err := hex.DecodeString(hexString)
		if err != nil {
			fmt.Printf("Invalid hex response from peer %s for block %d\n", peer, blockNumber)
			return false, nil
		}

		fmt.Printf("Successfully fetched root hash from peer %s for block %d\n", peer, blockNumber)
		return true, rootHash
	} else {
		fmt.Printf("Peer %s returned HTTP %d for block %d\n", peer, resp.StatusCode, blockNumber)
		return true, nil
	}
}

// checkRootHashValidityAndSave validates the local Merkle root against peers and persists it if a quorum of peers agree
func checkRootHashValidityAndSave(blockNumber int64, peers []string) {
	localRoot, _ := dbservice.GetRootHash()
	if localRoot == nil {
		fmt.Printf("No local root hash available for block %d\n", blockNumber)
		return
	}

	peersCount := len(peers)
	quorum := (peersCount*2)/3 + 1
	matches := 0

	for _, peer := range peers {
		success, peerRoot := fetchPeerRootHash(peer, blockNumber)

		if success && peerRoot != nil {
			if string(peerRoot) == string(localRoot) {
				matches++
			}
		} else {
			peersCount--
			quorum = (peersCount*2)/3 + 1
		}

		if matches >= quorum {
			dbservice.SetBlockRootHash(blockNumber, localRoot)
			fmt.Printf("Root hash validated and saved for block %d\n", blockNumber)
			return
		}
	}

	fmt.Printf("Root hash mismatch: only %d/%d peers agreed\n", matches, len(peers))

	// Revert changes and reset block to reprocess the data
	dbservice.RevertUnsavedChanges()
}

// handleTransfer executes a token transfer described by the given JSON payload
func handleTransfer(jsonData map[string]interface{}, senderHex string) {
	// Extract amount and receiver from JSON
	amountRaw := jsonData["amount"]
	receiverHex, _ := jsonData["receiver"].(string)

	if amountRaw == nil || receiverHex == "" {
		fmt.Printf("Skipping invalid transfer: %v\n", jsonData)
		return
	}

	// Convert amount to big.Int
	var amount *big.Int
	switch v := amountRaw.(type) {
	case string:
		amount, _ = new(big.Int).SetString(v, 10)
	case float64:
		amount = big.NewInt(int64(v))
	default:
		fmt.Printf("Invalid amount type: %v\n", jsonData)
		return
	}

	// Decode hex addresses
	senderAddress := strings.TrimPrefix(senderHex, "0x")
	receiverAddress := strings.TrimPrefix(receiverHex, "0x")

	sender, _ := hex.DecodeString(senderAddress)
	receiver, _ := hex.DecodeString(receiverAddress)

	// Execute transfer
	success, _ := dbservice.Transfer(sender, receiver, amount)

	if success {
		fmt.Printf("Transfer succeeded: %s from %s to %s\n", amount, senderHex, receiverHex)
	} else {
		fmt.Printf("Transfer failed (insufficient funds): %s from %s to %s\n", amount, senderHex, receiverHex)
	}
}

// processTransaction processes a single VIDA transaction
func processTransaction(transaction rpc.VidaDataTransaction) {
	fmt.Printf("TRANSACTION RECEIVED: %s\n", transaction.Data)

	// Get transaction data and convert from hex to bytes
	dataBytes, _ := hex.DecodeString(transaction.Data)

	// Parse JSON data
	var jsonData map[string]interface{}
	json.Unmarshal(dataBytes, &jsonData)

	// Get action from JSON
	action, _ := jsonData["action"].(string)

	if strings.ToLower(action) == "transfer" {
		handleTransfer(jsonData, transaction.Sender)
	}
}

// onChainProgress callback invoked as blocks are processed
func onChainProgress(blockNumber int64, peers []string) {
	dbservice.SetLastCheckedBlock(blockNumber)
	checkRootHashValidityAndSave(blockNumber, peers)
	fmt.Printf("Checkpoint updated to block %d\n", blockNumber)
	dbservice.Flush()
}

// subscribeAndSync subscribes to VIDA transactions starting from the given block
func subscribeAndSync(fromBlock int, peers []string) {
	fmt.Printf("Starting VIDA transaction subscription from block %d\n", fromBlock)

	// Initialize RPC client
	rpcClient := rpc.SetRpcNodeUrl(RPC_URL)

	subscription = rpcClient.SubscribeToVidaTransactions(
		VIDA_ID,
		fromBlock,
		processTransaction,
	)

	fmt.Printf("Successfully subscribed to VIDA %d transactions\n", VIDA_ID)

	// Start monitoring loop for block progress in a separate goroutine
	go func() {
		lastChecked, _ := dbservice.GetLastCheckedBlock()

		for {
			currentBlock := subscription.GetLatestCheckedBlock()

			if currentBlock > int(lastChecked) {
				onChainProgress(int64(currentBlock), peers)
				lastChecked = int64(currentBlock)
			}

			time.Sleep(5 * time.Second)
		}
	}()

	fmt.Println("Block progress monitor started")
}
