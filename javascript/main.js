import PWRJS from "@pwrjs/core";
import express from 'express';
import fetch from 'node-fetch';
import { GET } from './api/get.js';
import DatabaseService from './databaseService.js';

/**
 * Main application class that orchestrates the entire system.
 * JavaScript equivalent of the Rust Main struct.
 */
class Main {
    constructor() {
        this.vidaId = 73746238;
        this.startBlock = 1;
        this.rpcUrl = "https://pwrrpc.pwrlabs.io/";
        this.defaultPort = 8080;
        this.requestTimeoutSecs = 10;
        
        // Initial balances for fresh database (equivalent to Rust INITIAL_BALANCES)
        this.initialBalances = new Map([
            [Buffer.from("c767ea1d613eefe0ce1610b18cb047881bafb829", 'hex'), 1000000000000n],
            [Buffer.from("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4", 'hex'), 1000000000000n],
            [Buffer.from("9282d39ca205806473f4fde5bac48ca6dfb9d300", 'hex'), 1000000000000n],
            [Buffer.from("E68191B7913E72E6F1759531FBFAA089FF02308A", 'hex'), 1000000000000n]
        ]);
        
        // Instance variables
        this.pwrjsClient = null;
        this.peersToCheckRootHashWith = [];
        this.port = 8080;
        this.app = null;
        this.subscription = null;
        this.blockProgressMonitor = null;
    }
    
    /**
     * Parse command line arguments (equivalent to Rust parse_command_line_args)
     */
    parseCommandLineArgs() {
        const args = process.argv.slice(2);
        
        // Parse port if provided (simple implementation)
        if (args.length > 0) {
            const portArg = parseInt(args[0], 10);
            if (!isNaN(portArg)) {
                this.port = portArg;
                console.log(`Using port from args: ${this.port}`);
            }
        }
        
        // Parse peers (everything after port or from index 0)
        const peerStart = (args.length > 0 && !isNaN(parseInt(args[0], 10))) ? 1 : 0;
        if (args.length > peerStart) {
            this.peersToCheckRootHashWith = args.slice(peerStart);
        }
    }
    
    /**
     * Sets up the initial account balances when starting from a fresh database.
     * Equivalent to Rust init_initial_balances() method.
     */
    async initInitialBalances() {
        const lastCheckedBlock = await DatabaseService.getLastCheckedBlock();
        
        if (lastCheckedBlock === 0) {
            console.log("Setting up initial balances for fresh database");
            
            for (const [address, balance] of this.initialBalances) {
                await DatabaseService.setBalance(address, balance);
                console.log(`Set initial balance for ${address.toString('hex')}: ${balance}`);
            }
            
            // Flush to ensure balances are persisted
            await DatabaseService.flush();
            console.log("Initial balances setup completed");
        } else {
            console.log(`Database already initialized. Resuming from block ${lastCheckedBlock}`);
        }
    }
    
    /**
     * Initializes peer list from arguments or defaults.
     * Equivalent to Rust initialize_peers() method.
     */
    initializePeers() {
        if (this.peersToCheckRootHashWith.length === 0) {
            this.peersToCheckRootHashWith = [
                "localhost:8080"
            ];
            console.log("Using default peers:", this.peersToCheckRootHashWith);
        } else {
            console.log("Using peers from args:", this.peersToCheckRootHashWith);
        }
    }
    
    /**
     * Start the API server
     * Equivalent to Rust start_api_server() method.
     */
    async startApiServer() {
        this.app = express();
        
        // Register routes using class-based approach like Rust
        GET.run(this.app);
        
        return new Promise((resolve, reject) => {
            const server = this.app.listen(this.port, '0.0.0.0', (err) => {
                if (err) {
                    reject(err);
                } else {
                    console.log(`Starting API server on port ${this.port}`);
                    // Give server time to start (like Rust)
                    setTimeout(() => {
                        console.log(`API server started on http://0.0.0.0:${this.port}`);
                        resolve(server);
                    }, 2000);
                }
            });
        });
    }
    
    /**
     * Subscribes to VIDA transactions starting from the given block.
     * Equivalent to Rust subscribe_and_sync() method.
     */
    async subscribeAndSync(fromBlock) {
        console.log(`Starting VIDA transaction subscription from block ${fromBlock}`);
        
        // Initialize RPC client
        this.pwrjsClient = new PWRJS(this.rpcUrl);
        
        // Subscribe to VIDA transactions using pwrjs (like Rust pwr_rs)
        this.subscription = this.pwrjsClient.subscribeToVidaTransactions(
            this.vidaId,
            BigInt(fromBlock),
            (txn) => Main.processTransaction(txn) // Transaction handler callback
        );
        
        console.log(`Successfully subscribed to VIDA ${this.vidaId} transactions`);
        
        // Start monitoring loop for block progress (equivalent to Rust _start_block_progress_monitor)
        this.startBlockProgressMonitor(fromBlock);
        
        console.log("Block progress monitor started");
    }
    
    /**
     * Start block progress monitoring (equivalent to Rust tokio::spawn block)
     */
    startBlockProgressMonitor(startBlock) {
        let lastChecked = startBlock;
        
        this.blockProgressMonitor = setInterval(async () => {
            try {
                // Simulate Rust's subscription.get_latest_checked_block() by getting latest chain block
                // and processing incrementally like Rust does (by chunks)
                const latestChainBlock = await this.getLatestChainBlock();
                
                // Process blocks incrementally like Rust (similar to Rust's chunked processing)
                if (lastChecked < latestChainBlock) {
                    // Increment by reasonable chunks like Rust (process blocks in batches)
                    const nextBlock = Math.min(lastChecked + 9009, latestChainBlock); // Similar to Rust's increment
                    
                    if (nextBlock > lastChecked) {
                        try {
                            await Main.onChainProgress(nextBlock, this.peersToCheckRootHashWith);
                            lastChecked = nextBlock;
                        } catch (error) {
                            console.error("Error in chain progress:", error);
                        }
                    }
                }
            } catch (error) {
                console.error("Error in block progress monitor:", error);
            }
        }, 5000); // Check every 5 seconds like Rust
    }
    
    /**
     * Get latest block from the chain (simulates what Rust subscription provides)
     */
    async getLatestChainBlock() {
        try {
            // Get the latest block number from the PWR chain
            const latestBlock = await this.pwrjsClient.getLatestBlockNumber();
            return Number(latestBlock);
        } catch (error) {
            console.error("Failed to get latest chain block:", error);
            // Fallback to database value
            return await DatabaseService.getLastCheckedBlock();
        }
    }
    
    /**
     * Callback invoked as blocks are processed.
     * Equivalent to Rust on_chain_progress() method.
     */
    static async onChainProgress(blockNumber, peers) {
        try {
            await DatabaseService.setLastCheckedBlock(blockNumber);
            await Main.checkRootHashValidityAndSave(blockNumber, peers);
            console.log(`Checkpoint updated to block ${blockNumber}`);
            
            // Flush changes to disk after each checkpoint like Rust
            await DatabaseService.flush();
        } catch (error) {
            console.error(`Error in chain progress for block ${blockNumber}:`, error.message);
            throw error;
        }
    }
    
    /**
     * Processes a single VIDA transaction.
     * Equivalent to Rust process_transaction() method.
     */
    static processTransaction(txn) {
        console.log(`TRANSACTION RECEIVED: ${txn.data}`);
        
        try {
            Main.handleTransaction(txn);
        } catch (error) {
            console.error("Error processing transaction:", error);
        }
    }
    
    /**
     * Handle transaction processing
     */
    static handleTransaction(txn) {
        // Get transaction data and convert from hex to bytes (like Rust)
        const dataBytes = Buffer.from(txn.data, 'hex');
        
        // Parse JSON data
        const dataStr = dataBytes.toString('utf8');
        const jsonData = JSON.parse(dataStr);
        
        // Get action from JSON
        const action = jsonData.action || "";
        
        if (action.toLowerCase() === "transfer") {
            Main.handleTransfer(jsonData, txn.sender);
        } else {
            console.log(`Ignoring transaction with action: ${action}`);
        }
    }
    
    /**
     * Executes a token transfer described by the given JSON payload.
     * Equivalent to Rust handle_transfer() method.
     */
    static async handleTransfer(jsonData, senderHex) {
        // Extract amount and receiver from JSON
        let amount;
        if (typeof jsonData.amount === 'string') {
            amount = BigInt(jsonData.amount);
        } else if (typeof jsonData.amount === 'number') {
            amount = BigInt(jsonData.amount);
        } else {
            throw new Error("Invalid or missing amount");
        }
        
        const receiverHex = jsonData.receiver;
        if (!receiverHex) {
            throw new Error("Missing receiver");
        }
        
        // Decode hex addresses
        const sender = Main.decodeHexAddress(senderHex);
        const receiver = Main.decodeHexAddress(receiverHex);
        
        // Execute transfer
        const success = await DatabaseService.transfer(sender, receiver, amount);
        
        if (success) {
            console.log(`Transfer succeeded: ${amount} from ${senderHex} to ${receiverHex}`);
        } else {
            console.log(`Transfer failed (insufficient funds):`, jsonData);
        }
    }
    
    /**
     * Decodes a hexadecimal address into raw bytes.
     * Equivalent to Rust decode_hex_address() method.
     */
    static decodeHexAddress(hexStr) {
        // Remove '0x' prefix if present
        const cleanHex = hexStr.startsWith("0x") ? hexStr.slice(2) : hexStr;
        return Buffer.from(cleanHex, 'hex');
    }
    
    /**
     * Validates the local Merkle root against peers and persists it if a quorum
     * of peers agree. Equivalent to Rust check_root_hash_validity_and_save() method.
     */
    static async checkRootHashValidityAndSave(blockNumber, peers) {
        const localRoot = await DatabaseService.getRootHash();
        
        if (!localRoot) {
            console.log(`No local root hash available for block ${blockNumber}`);
            return;
        }
        
        let peersCount = peers.length;
        let quorum = Math.floor((peersCount * 2) / 3) + 1;
        let matches = 0;
        
        for (const peer of peers) {
            const [success, peerRoot] = await Main.fetchPeerRootHash(peer, blockNumber);
            
            if (success && peerRoot) {
                if (Buffer.compare(peerRoot, localRoot) === 0) {
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
        
        console.error(`Root hash mismatch: only ${matches}/${peers.length} peers agreed`);
        
        // Revert changes and reset block to reprocess the data
        await DatabaseService.revertUnsavedChanges();
        // Note: In real implementation, you'd reset the subscription
    }
    
    /**
     * Fetches the root hash from a peer node for the specified block number.
     * Equivalent to Rust fetch_peer_root_hash() method.
     */
    static async fetchPeerRootHash(peer, blockNumber) {
        const url = `http://${peer}/rootHash?blockNumber=${blockNumber}`;
        
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
            
            const response = await fetch(url, {
                headers: {
                    'Accept': 'text/plain' // Match Rust's expectation
                },
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            if (response.ok) {
                const hexString = await response.text();
                const trimmed = hexString.trim();
                
                if (trimmed === '') {
                    console.log(`Peer ${peer} returned empty root hash for block ${blockNumber}`);
                    return [false, null];
                }
                
                try {
                    const rootHash = Buffer.from(trimmed, 'hex');
                    console.log(`Successfully fetched root hash from peer ${peer} for block ${blockNumber}`);
                    return [true, rootHash];
                } catch (error) {
                    console.log(`Invalid hex response from peer ${peer} for block ${blockNumber}:`, error);
                    return [false, null];
                }
            } else {
                console.log(`Peer ${peer} returned HTTP ${response.status} for block ${blockNumber}`);
                return [true, null]; // Peer responded but with error
            }
        } catch (error) {
            if (error.name === 'AbortError') {
                console.log(`Request timeout for peer ${peer} block ${blockNumber}`);
            } else {
                console.log(`Failed to fetch root hash from peer ${peer} for block ${blockNumber}:`, error);
            }
            return [false, null];
        }
    }
    
    /**
     * Main application entry point that orchestrates the entire system.
     * Equivalent to Rust run() method.
     */
    async run() {
        console.log("=== Starting PWR VIDA Synchronizer ===");
        
        try {
            // Parse command line arguments
            this.parseCommandLineArgs();
            
            // Initialize database service
            await DatabaseService.initialize();
            
            // Start API server
            await this.startApiServer();
            
            // Initialize initial balances
            await this.initInitialBalances();
            
            // Initialize peers
            this.initializePeers();
            
            // Determine starting block
            const lastBlock = await DatabaseService.getLastCheckedBlock();
            const fromBlock = lastBlock > 0 ? lastBlock : this.startBlock;
            
            console.log(`Starting synchronization from block ${fromBlock}`);
            
            // Subscribe and sync
            await this.subscribeAndSync(fromBlock);
            
            // Keep the main thread alive
            console.log("Application started successfully. Press Ctrl+C to exit.");
            
            // Set up graceful shutdown handlers
            this.setupShutdownHandlers();
            
            // Keep process alive
            await this.keepAlive();
            
        } catch (error) {
            console.error("Application failed to start:", error);
            await this.shutdown();
            process.exit(1);
        }
    }
    
    /**
     * Setup graceful shutdown handlers
     */
    setupShutdownHandlers() {
        const gracefulShutdown = async (signal) => {
            console.log(`\nReceived ${signal}. Shutting down gracefully...`);
            await this.shutdown();
            process.exit(0);
        };
        
        process.on('SIGINT', () => gracefulShutdown('SIGINT'));
        process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
        process.on('uncaughtException', async (error) => {
            console.error('Uncaught Exception:', error);
            await this.shutdown();
            process.exit(1);
        });
        process.on('unhandledRejection', async (reason, promise) => {
            console.error('Unhandled Rejection at:', promise, 'reason:', reason);
            await this.shutdown();
            process.exit(1);
        });
    }
    
    /**
     * Keep the process alive (equivalent to Rust's tokio::signal::ctrl_c().await)
     */
    async keepAlive() {
        return new Promise((resolve) => {
            // This will be resolved by the shutdown handlers
            process.on('SIGINT', resolve);
            process.on('SIGTERM', resolve);
        });
    }
    
    /**
     * Graceful application shutdown
     */
    async shutdown() {
        console.log("Shutting down application...");
        
        try {
            // Stop block progress monitor
            if (this.blockProgressMonitor) {
                clearInterval(this.blockProgressMonitor);
                this.blockProgressMonitor = null;
            }
            
            // Stop subscription
            if (this.subscription && typeof this.subscription.stop === 'function') {
                this.subscription.stop();
                this.subscription = null;
            }
            
            // Flush any pending database changes
            await DatabaseService.flush();
            console.log("Flushed database changes");
            
            // Close database service
            await DatabaseService.close();
            console.log("Closed database service");
            
        } catch (error) {
            console.error("Error during shutdown:", error);
        }
        
        console.log("Application shutdown complete");
    }
}

/**
 * Application entry point.
 * Equivalent to Rust main() function
 */
async function main() {
    const app = new Main();
    await app.run();
}

// Run the application if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch((error) => {
        console.error('Fatal error:', error);
        process.exit(1);
    });
}
