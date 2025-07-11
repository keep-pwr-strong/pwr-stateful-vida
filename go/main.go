package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"pwr-stateful-vida/api"
	"pwr-stateful-vida/dbservice"
	"github.com/pwrlabs/pwrgo/rpc"
)

// Constants
const (
	VIDA_ID     = 73746238
	START_BLOCK = 1
	PORT        = 8080
	RPC_URL     = "https://pwrrpc.pwrlabs.io"
)

// Global state
var (
	peersToCheckRootHashWith = []string{"localhost:8080"}
	subscription             *rpc.VidaTransactionSubscription
	rpcClient                *rpc.RPC
)

// main is the application entry point for synchronizing VIDA transactions
func main() {
	fmt.Println("Starting PWR VIDA Transaction Synchronizer...")

	// Initialize peers from command line arguments
	initializePeers()

	// Initialize RPC client
	rpcClient = rpc.SetRpcNodeUrl(RPC_URL)

	// Set up HTTP API server
	go startAPIServer()

	// Initialize database with initial balances if needed
	initInitialBalances()

	// Get starting block number
	lastBlock, _ := dbservice.GetLastCheckedBlock()
	fromBlock := START_BLOCK
	if lastBlock > 0 {
		fromBlock = int(lastBlock)
	}

	fmt.Printf("Starting synchronization from block %d\n", fromBlock)

	// Subscribe to VIDA transactions
	subscribeAndSync(fromBlock)

	// Wait for shutdown signal
	waitForShutdown()
}

// startAPIServer initializes and starts the HTTP API server
func startAPIServer() {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	api.RegisterRoutes(router)

	fmt.Printf("Starting HTTP server on port %d\n", PORT)
	router.Run(fmt.Sprintf(":%d", PORT))
}

// initInitialBalances sets up the initial account balances when starting from a fresh database
func initInitialBalances() {
	lastBlock, _ := dbservice.GetLastCheckedBlock()
	if lastBlock == 0 {
		fmt.Println("Setting up initial balances for fresh database")

		initialBalances := map[string]*big.Int{
			"c767ea1d613eefe0ce1610b18cb047881bafb829": big.NewInt(1000000000000),
			"3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4": big.NewInt(1000000000000),
			"9282d39ca205806473f4fde5bac48ca6dfb9d300": big.NewInt(1000000000000),
			"e68191b7913e72e6f1759531fbfaa089ff02308a": big.NewInt(1000000000000),
		}

		for addressHex, balance := range initialBalances {
			address, _ := hex.DecodeString(addressHex)
			dbservice.SetBalance(address, balance)
		}

		fmt.Println("Initial balances setup completed")
	}
}

// initializePeers initializes peer list from arguments or defaults
func initializePeers() {
	if len(os.Args) > 1 {
		peersToCheckRootHashWith = os.Args[1:]
		fmt.Printf("Using peers from args: %v\n", peersToCheckRootHashWith)
	} else {
		fmt.Printf("Using default peers: %v\n", peersToCheckRootHashWith)
	}
}

// subscribeAndSync subscribes to VIDA transactions starting from the given block
func subscribeAndSync(fromBlock int) {
	fmt.Printf("Starting VIDA transaction subscription from block %d\n", fromBlock)

	subscription = rpcClient.SubscribeToVidaTransactions(
		VIDA_ID,
		fromBlock,
		processTransaction,
	)

	fmt.Printf("Successfully subscribed to VIDA %d transactions\n", VIDA_ID)

	// Set up block progress monitoring
	go monitorBlockProgress()
	fmt.Println("Block progress monitor started")
}

// monitorBlockProgress monitors the subscription progress and handles block checkpoints
func monitorBlockProgress() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastReportedBlock int64 = 0

	for {
		select {
		case <-ticker.C:
			if subscription != nil && subscription.IsRunning() {
				currentBlock := int64(subscription.GetLatestCheckedBlock())

				if currentBlock > lastReportedBlock {
					onChainProgress(currentBlock)
					lastReportedBlock = currentBlock
				}
			}
		}
	}
}

// onChainProgress callback invoked as blocks are processed
func onChainProgress(blockNumber int64) {
	dbservice.SetLastCheckedBlock(blockNumber)
	checkRootHashValidityAndSave(blockNumber)
	fmt.Printf("Checkpoint updated to block %d\n", blockNumber)
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
	sender := decodeHexAddress(senderHex)
	receiver := decodeHexAddress(receiverHex)

	// Execute transfer
	success, _ := dbservice.Transfer(sender, receiver, amount)

	if success {
		fmt.Printf("Transfer succeeded: %s from %s to %s\n", amount, senderHex, receiverHex)
	} else {
		fmt.Printf("Transfer failed (insufficient funds): %s from %s to %s\n", amount, senderHex, receiverHex)
	}
}

// decodeHexAddress decodes a hexadecimal address into raw bytes
func decodeHexAddress(hexAddr string) []byte {
	cleanHex := strings.TrimPrefix(hexAddr, "0x")
	address, _ := hex.DecodeString(cleanHex)
	return address
}

// checkRootHashValidityAndSave validates the local Merkle root against peers and persists it if a quorum of peers agree
func checkRootHashValidityAndSave(blockNumber int64) {
	localRoot, _ := dbservice.GetRootHash()
	if localRoot == nil {
		fmt.Printf("No local root hash available for block %d\n", blockNumber)
		return
	}

	peersCount := len(peersToCheckRootHashWith)
	quorum := (peersCount*2)/3 + 1
	matches := 0

	for _, peer := range peersToCheckRootHashWith {
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

	fmt.Printf("Root hash mismatch: only %d/%d peers agreed\n", matches, len(peersToCheckRootHashWith))

	// Revert changes and reset block to reprocess the data
	dbservice.RevertUnsavedChanges()
}

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

// waitForShutdown waits for shutdown signal
func waitForShutdown() {
	fmt.Println("Application started successfully. Press Ctrl+C to exit.")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	fmt.Println("Shutting down application...")

	// Stop subscription
	if subscription != nil {
		subscription.Stop()
	}

	// Flush any pending database changes
	dbservice.Flush()
	fmt.Println("Flushed database changes")

	fmt.Println("Application shutdown complete")
}
