package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/keep-pwr-strong/pwr-stateful-vida/api"
	"github.com/keep-pwr-strong/pwr-stateful-vida/database"
	"github.com/pwrlabs/pwrgo/rpc"
)

const (
	VIDA_ID     = 73746238
	START_BLOCK = 1
	PORT        = 8080
	RPC_URL     = "https://pwrrpc.pwrlabs.io"
)

var (
	logger                   = log.New(os.Stdout, "[VIDA-SYNC] ", log.LstdFlags|log.Lshortfile)
	peersToCheckRootHashWith []string
	subscription             *rpc.VidaTransactionSubscription
	httpClient               = &http.Client{Timeout: 10 * time.Second}
	rpcClient                *rpc.RPC
)

// BiResult represents a result with two values (similar to Java BiResult)
type BiResult[T any, U any] struct {
	First  T
	Second U
}

// TransferData represents the JSON structure for transfer transactions
type TransferData struct {
	Action   string   `json:"action"`
	Amount   *big.Int `json:"amount"`
	Receiver string   `json:"receiver"`
}

// UnmarshalJSON custom unmarshaling for big.Int
func (td *TransferData) UnmarshalJSON(data []byte) error {
	// First, unmarshal into a map to handle flexible types
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Handle action
	if action, ok := raw["action"].(string); ok {
		td.Action = action
	}

	// Handle receiver
	if receiver, ok := raw["receiver"].(string); ok {
		td.Receiver = receiver
	}

	// Handle amount - can be string or number
	if amountRaw, exists := raw["amount"]; exists && amountRaw != nil {
		amount := new(big.Int)
		
		switch v := amountRaw.(type) {
		case string:
			// Handle string format
			if v != "" {
				if _, ok := amount.SetString(v, 10); !ok {
					return fmt.Errorf("invalid amount string format: %s", v)
				}
			}
		case float64:
			// Handle number format (JSON numbers are float64)
			amount.SetInt64(int64(v))
		case int:
			// Handle int format
			amount.SetInt64(int64(v))
		case int64:
			// Handle int64 format
			amount.SetInt64(v)
		default:
			return fmt.Errorf("invalid amount type: %T", v)
		}
		
		td.Amount = amount
	}

	return nil
}

// main is the application entry point for synchronizing VIDA transactions
// with the local Merkle-backed database.
//
// Args: optional list of peer hosts to query for root hash validation
func main() {
	logger.Println("Starting PWR VIDA Transaction Synchronizer...")

	// Initialize peers from command line arguments
	initializePeers(os.Args[1:])

	// Initialize RPC client
	rpcClient = rpc.SetRpcNodeUrl(RPC_URL)

	// Set up HTTP API server
	go startAPIServer()

	// Initialize database with initial balances if needed
	if err := initInitialBalances(); err != nil {
		logger.Fatalf("Failed to initialize balances: %v", err)
	}

	// Get starting block number
	lastBlock, err := database.GetLastCheckedBlock()
	if err != nil {
		logger.Fatalf("Failed to get last checked block: %v", err)
	}

	fromBlock := START_BLOCK
	if lastBlock > 0 {
		fromBlock = int(lastBlock)
	}

	logger.Printf("Starting synchronization from block %d", fromBlock)

	// Subscribe to VIDA transactions
	if err := subscribeAndSync(fromBlock); err != nil {
		logger.Fatalf("Failed to start subscription: %v", err)
	}

	// Wait for shutdown signal
	waitForShutdown()
}

// startAPIServer initializes and starts the HTTP API server
func startAPIServer() {
	// Use Gin in release mode for production
	gin.SetMode(gin.ReleaseMode)
	
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	// Register API routes
	api.RegisterRoutes(router)

	logger.Printf("Starting HTTP server on port %d", PORT)
	if err := router.Run(fmt.Sprintf(":%d", PORT)); err != nil {
		logger.Fatalf("Failed to start HTTP server: %v", err)
	}
}

// initInitialBalances sets up the initial account balances when starting from a fresh database
func initInitialBalances() error {
	lastBlock, err := database.GetLastCheckedBlock()
	if err != nil {
		return fmt.Errorf("failed to get last checked block: %w", err)
	}

	if lastBlock == 0 {
		logger.Println("Initializing fresh database with initial balances...")

		initialBalances := map[string]*big.Int{
			"c767ea1d613eefe0ce1610b18cb047881bafb829": big.NewInt(1000000000000),
			"3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4": big.NewInt(1000000000000),
			"9282d39ca205806473f4fde5bac48ca6dfb9d300": big.NewInt(1000000000000),
			"E68191B7913E72E6F1759531FBFAA089FF02308A": big.NewInt(1000000000000),
		}

		for addressHex, balance := range initialBalances {
			address, err := hex.DecodeString(addressHex)
			if err != nil {
				return fmt.Errorf("failed to decode address %s: %w", addressHex, err)
			}

			if err := database.SetBalance(address, balance); err != nil {
				return fmt.Errorf("failed to set balance for %s: %w", addressHex, err)
			}
		}

		logger.Println("Initial balances set successfully")
	}

	return nil
}

// initializePeers initializes peer list from arguments or defaults
func initializePeers(args []string) {
	if len(args) > 0 {
		peersToCheckRootHashWith = args
		logger.Printf("Using peers from args: %v", peersToCheckRootHashWith)
	} else {
		peersToCheckRootHashWith = []string{
			"localhost:8080",  // Your own node for testing
			// Add real PWR node addresses here when available
		}
		logger.Printf("Using default peers: %v", peersToCheckRootHashWith)
	}
}

// subscribeAndSync subscribes to VIDA transactions starting from the given block
func subscribeAndSync(fromBlock int) error {
	logger.Printf("Subscribing to VIDA %d transactions from block %d", VIDA_ID, fromBlock)

	subscription = rpcClient.SubscribeToVidaTransactions(
		VIDA_ID,
		fromBlock,
		processTransaction,
	)

	// Set up block progress monitoring
	go monitorBlockProgress()

	return nil
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
					if err := onChainProgress(currentBlock); err != nil {
						logger.Printf("Error processing block progress %d: %v", currentBlock, err)
					}
					lastReportedBlock = currentBlock
				}
			}
		}
	}
}

// onChainProgress callback invoked as blocks are processed
func onChainProgress(blockNumber int64) error {
	if err := database.SetLastCheckedBlock(blockNumber); err != nil {
		return fmt.Errorf("failed to set last checked block: %w", err)
	}

	if err := checkRootHashValidityAndSave(blockNumber); err != nil {
		logger.Printf("Root hash validation failed for block %d: %v", blockNumber, err)
		return err
	}

	logger.Printf("Checkpoint updated to block %d", blockNumber)
	return nil
}

// processTransaction processes a single VIDA transaction
func processTransaction(transaction rpc.VidaDataTransaction) {
	logger.Printf("Processing transaction: %+v", transaction)
	logger.Printf("Processing transaction from %s", transaction.Sender)

	// Convert hex data to bytes
	dataBytes, err := hex.DecodeString(transaction.Data)
	if err != nil {
		logger.Printf("Error decoding transaction data: %v", err)
		return
	}

	// Log the raw JSON for debugging
	logger.Printf("Raw transaction data: %s", string(dataBytes))

	// Parse JSON data
	var transferData TransferData
	if err := json.Unmarshal(dataBytes, &transferData); err != nil {
		logger.Printf("Error parsing transaction JSON: %v", err)
		logger.Printf("Raw JSON was: %s", string(dataBytes))
		return
	}

	logger.Printf("Parsed transfer: action=%s, amount=%s, receiver=%s", 
		transferData.Action, 
		transferData.Amount.String(), 
		transferData.Receiver)

	// Handle transfer action
	if strings.EqualFold(transferData.Action, "transfer") {
		if err := handleTransfer(transferData, transaction.Sender); err != nil {
			logger.Printf("Error handling transfer: %v", err)
		}
	} else {
		logger.Printf("Unknown action: %s", transferData.Action)
	}
}

// handleTransfer executes a token transfer described by the given transfer data
func handleTransfer(transferData TransferData, senderHex string) error {
	if transferData.Amount == nil || transferData.Receiver == "" {
		logger.Printf("Skipping invalid transfer: amount=%v, receiver=%s", transferData.Amount, transferData.Receiver)
		return nil
	}

	sender, err := decodeHexAddress(senderHex)
	if err != nil {
		return fmt.Errorf("failed to decode sender address: %w", err)
	}

	receiver, err := decodeHexAddress(transferData.Receiver)
	if err != nil {
		return fmt.Errorf("failed to decode receiver address: %w", err)
	}

	success, err := database.Transfer(sender, receiver, transferData.Amount)
	if err != nil {
		return fmt.Errorf("transfer operation failed: %w", err)
	}

	if success {
		logger.Printf("Transfer succeeded: %s -> %s, amount: %s", 
			senderHex, transferData.Receiver, transferData.Amount.String())
	} else {
		logger.Printf("Transfer failed (insufficient funds): %s -> %s, amount: %s", 
			senderHex, transferData.Receiver, transferData.Amount.String())
	}

	return nil
}

// decodeHexAddress decodes a hexadecimal address into raw bytes
func decodeHexAddress(hexAddr string) ([]byte, error) {
	clean := strings.TrimPrefix(hexAddr, "0x")
	return hex.DecodeString(clean)
}

// checkRootHashValidityAndSave validates the local Merkle root against peers
// and persists it if a quorum of peers agree
func checkRootHashValidityAndSave(blockNumber int64) error {
	localRoot, err := database.GetRootHash()
	if err != nil {
		return fmt.Errorf("failed to get local root hash: %w", err)
	}

	peersCount := len(peersToCheckRootHashWith)
	quorum := (peersCount*2)/3 + 1
	matches := 0

	for _, peer := range peersToCheckRootHashWith {
		result := fetchPeerRootHash(peer, blockNumber)
		
		if result.First { // Successfully contacted peer
			if bytes.Equal(result.Second, localRoot) {
				matches++
			}
		} else {
			peersCount-- // Reduce peer count if peer is unreachable
			quorum = (peersCount*2)/3 + 1
		}

		if matches >= quorum {
			if err := database.SetBlockRootHash(blockNumber, localRoot); err != nil {
				return fmt.Errorf("failed to save block root hash: %w", err)
			}
			logger.Printf("Root hash validated and saved for block %d", blockNumber)
			return nil
		}
	}

	logger.Printf("Root hash mismatch: only %d/%d peers agreed", matches, len(peersToCheckRootHashWith))
	
	// Revert changes and reset block to reprocess the data
	if err := database.RevertUnsavedChanges(); err != nil {
		return fmt.Errorf("failed to revert unsaved changes: %w", err)
	}

	// Reset subscription to last known good block
	if subscription != nil {
		lastGoodBlock, err := database.GetLastCheckedBlock()
		if err != nil {
			return fmt.Errorf("failed to get last checked block: %w", err)
		}
		
		// Note: PWR Go client might need a method to reset the subscription
		// This is a placeholder - actual implementation depends on the client
		logger.Printf("Would reset subscription to block %d", lastGoodBlock)
	}

	return fmt.Errorf("consensus validation failed")
}

// fetchPeerRootHash fetches the root hash from a peer node for the specified block number
func fetchPeerRootHash(peer string, blockNumber int64) BiResult[bool, []byte] {
	url := fmt.Sprintf("http://%s/rootHash?blockNumber=%d", peer, blockNumber)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logger.Printf("Failed to create request for peer %s: %v", peer, err)
		return BiResult[bool, []byte]{First: false, Second: []byte{}}
	}

	req.Header.Set("Accept", "text/plain")

	resp, err := httpClient.Do(req)
	if err != nil {
		logger.Printf("Failed to contact peer %s: %v", peer, err)
		return BiResult[bool, []byte]{First: false, Second: []byte{}}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logger.Printf("Peer %s returned HTTP %d for block %d: %s", 
			peer, resp.StatusCode, blockNumber, string(body))
		return BiResult[bool, []byte]{First: true, Second: []byte{}}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Printf("Failed to read response from peer %s: %v", peer, err)
		return BiResult[bool, []byte]{First: false, Second: []byte{}}
	}

	hexString := strings.TrimSpace(string(body))
	if hexString == "" {
		logger.Printf("Peer %s returned empty root hash for block %d", peer, blockNumber)
		return BiResult[bool, []byte]{First: false, Second: []byte{}}
	}

	rootHash, err := hex.DecodeString(hexString)
	if err != nil {
		logger.Printf("Invalid hex response from peer %s for block %d: %v", peer, blockNumber, err)
		return BiResult[bool, []byte]{First: false, Second: []byte{}}
	}

	logger.Printf("Successfully fetched root hash from peer %s for block %d", peer, blockNumber)
	return BiResult[bool, []byte]{First: true, Second: rootHash}
}

// waitForShutdown waits for shutdown signals and performs cleanup
func waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	logger.Println("VIDA synchronizer is running. Press Ctrl+C to stop...")
	<-sigChan

	logger.Println("Shutdown signal received, cleaning up...")

	// Stop subscription
	if subscription != nil && subscription.IsRunning() {
		logger.Println("Stopping VIDA subscription...")
		subscription.Stop()
	}

	// Flush any pending database changes
	if err := database.Flush(); err != nil {
		logger.Printf("Error flushing database: %v", err)
	}

	// Close database
	if err := database.Close(); err != nil {
		logger.Printf("Error closing database: %v", err)
	}

	logger.Println("Shutdown complete")
	os.Exit(0)
}