import express from 'express';
import { GET } from './api/get.js';
import DatabaseService from './databaseService.js';
import { subscribeAndSync, peersToCheckRootHashWith } from './handler.js';

const START_BLOCK = 1;
const PORT = 8080;

const INITIAL_BALANCES = new Map([
    [Buffer.from("c767ea1d613eefe0ce1610b18cb047881bafb829", 'hex'), 1000000000000n],
    [Buffer.from("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4", 'hex'), 1000000000000n],
    [Buffer.from("9282d39ca205806473f4fde5bac48ca6dfb9d300", 'hex'), 1000000000000n],
    [Buffer.from("e68191b7913e72e6f1759531fbfaa089ff02308a", 'hex'), 1000000000000n],
]);

let app = null;

// Initializes peer list from arguments or defaults
function initializePeers() {
    const args = process.argv.slice(2);
    
    if (args.length > 0) {
        peersToCheckRootHashWith.length = 0;
        peersToCheckRootHashWith.push(...args);
        console.log("Using peers from args:", peersToCheckRootHashWith);
    } else {
        peersToCheckRootHashWith.length = 0;
        peersToCheckRootHashWith.push("localhost:8080");
        console.log("Using default peers:", peersToCheckRootHashWith);
    }
}

// Sets up the initial account balances when starting from a fresh database
async function initInitialBalances() {
    const lastCheckedBlock = await DatabaseService.getLastCheckedBlock();
    
    if (lastCheckedBlock === 0) {
        console.log("Setting up initial balances for fresh database");
        
        for (const [address, balance] of INITIAL_BALANCES) {
            await DatabaseService.setBalance(address, balance);
            console.log(`Set initial balance for ${address.toString('hex')}: ${balance}`);
        }
        console.log("Initial balances setup completed");
    }
}

// Start the API server in a background task
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

// Sets up shutdown handlers for graceful shutdown
function setupShutdownHandlers() {
    const gracefulShutdown = async (signal) => {
        console.log(`Received ${signal}, shutting down gracefully...`);
        process.exit(0);
    };
    
    process.on('SIGINT', gracefulShutdown);
    process.on('SIGTERM', gracefulShutdown);
}

// Application entry point for synchronizing VIDA transactions
// with the local Merkle-backed database.
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

    // Keep the main thread alive
    console.log("Application started successfully. Press Ctrl+C to exit.");
    // Graceful shutdown
    setupShutdownHandlers();
}

main().catch(console.error);
