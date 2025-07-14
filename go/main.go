package main

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"pwr-stateful-vida/api"
	"pwr-stateful-vida/dbservice"
)

// Constants
const (
	VIDA_ID     = 73746238
	START_BLOCK = 1
	PORT        = 8080
	RPC_URL     = "https://pwrrpc.pwrlabs.io"
)

// Global state
var peersToCheckRootHashWith = []string{"localhost:8080"}

// initializePeers initializes peer list from arguments or defaults
func initializePeers() {
	if len(os.Args) > 1 {
		peersToCheckRootHashWith = os.Args[1:]
		fmt.Printf("Using peers from args: %v\n", peersToCheckRootHashWith)
	} else {
		fmt.Printf("Using default peers: %v\n", peersToCheckRootHashWith)
	}
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

// startAPIServer initializes and starts the HTTP API server
func startAPIServer() {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	api.RegisterRoutes(router)

	fmt.Printf("Starting HTTP server on port %d\n", PORT)
	router.Run(fmt.Sprintf(":%d", PORT))
}

// main is the application entry point for synchronizing VIDA transactions
func main() {
	fmt.Println("Starting PWR VIDA Transaction Synchronizer...")

	// Initialize peers from command line arguments
	initializePeers()

	// Set up HTTP API server
	go startAPIServer()

	// Initialize database with initial balances if needed
	initInitialBalances()

	// Get starting block number
	lastBlock, _ := dbservice.GetLastCheckedBlock()
	fromBlock := START_BLOCK
	if lastBlock > 0 { fromBlock = int(lastBlock) }

	fmt.Printf("Starting synchronization from block %d\n", fromBlock)

	// Subscribe to VIDA transactions
	subscribeAndSync(fromBlock, peersToCheckRootHashWith)

	// Keep the main thread alive
	fmt.Println("Application started successfully. Press Ctrl+C to exit.")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
