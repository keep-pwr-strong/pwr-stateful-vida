mod database_service;
mod api;

use pwr_rs::{
    RPC,
    transaction::types::VidaDataTransaction,
};
use std::sync::Arc;
use std::env;
use std::time::Duration;
use hex;
use serde_json::{Value, Map};
use num_bigint::BigUint;
use tokio::time::sleep;

use crate::database_service::DatabaseService;
use crate::api::GET;

// Constants
const VIDA_ID: u64 = 73_746_238;
const START_BLOCK: u64 = 1;
const RPC_URL: &str = "https://pwrrpc.pwrlabs.io/";
const PORT: u16 = 8080;

// Global state
static mut PEERS_TO_CHECK_ROOT_HASH_WITH: Vec<String> = Vec::new();
static mut PWR_CLIENT: Option<Arc<RPC>> = None;

/// Start the API server in a background task
async fn start_api_server() {
    let routes = GET::run();
    
    tokio::spawn(async move {
        println!("Starting API server on port {}", PORT);
        warp::serve(routes)
            .run(([0, 0, 0, 0], PORT))
            .await;
    });
    
    // Give server time to start
    sleep(Duration::from_millis(2000)).await;
    println!("API server started on http://0.0.0.0:{}", PORT);
}

/// Sets up the initial account balances when starting from a fresh database
async fn init_initial_balances() -> Result<(), Box<dyn std::error::Error>> {
    if DatabaseService::get_last_checked_block().map_err(|e| format!("Failed to get last checked block: {:?}", e))? == 0 {
        println!("Setting up initial balances for fresh database");
        
        let initial_balances = vec![
            (hex::decode("c767ea1d613eefe0ce1610b18cb047881bafb829").unwrap(), BigUint::from(1_000_000_000_000u64)),
            (hex::decode("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4").unwrap(), BigUint::from(1_000_000_000_000u64)),
            (hex::decode("9282d39ca205806473f4fde5bac48ca6dfb9d300").unwrap(), BigUint::from(1_000_000_000_000u64)),
            (hex::decode("e68191b7913e72e6f1759531fbfaa089ff02308a").unwrap(), BigUint::from(1_000_000_000_000u64)),
        ];
        
        for (address, balance) in initial_balances {
            DatabaseService::set_balance(&address, &balance).map_err(|e| format!("Failed to set balance: {:?}", e))?;
            println!("Set initial balance for {}: {}", hex::encode(&address), balance);
        }
        
        // Flush to ensure balances are persisted
        DatabaseService::flush().map_err(|e| format!("Failed to flush database: {:?}", e))?;
        println!("Initial balances setup completed");
    }
    
    Ok(())
}

/// Initializes peer list from arguments or defaults
fn initialize_peers() {
    let args: Vec<String> = env::args().collect();
    
    unsafe {
        if args.len() > 1 {
            PEERS_TO_CHECK_ROOT_HASH_WITH = args[1..].to_vec();
            println!("Using peers from args: {:?}", PEERS_TO_CHECK_ROOT_HASH_WITH);
        } else {
            PEERS_TO_CHECK_ROOT_HASH_WITH = vec![
                "localhost:8080".to_string(),
            ];
            println!("Using default peers: {:?}", PEERS_TO_CHECK_ROOT_HASH_WITH);
        }
    }
}

/// Subscribes to VIDA transactions starting from the given block
async fn subscribe_and_sync(from_block: u64) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting VIDA transaction subscription from block {}", from_block);
    
    // Initialize RPC client
    let rpc = RPC::new(RPC_URL).await.map_err(|e| format!("Failed to create RPC client: {:?}", e))?;
    let rpc = Arc::new(rpc);
    
    unsafe {
        PWR_CLIENT = Some(rpc.clone());
    }
    
    // Subscribe to VIDA transactions
    let subscription = rpc.subscribe_to_vida_transactions(
        VIDA_ID,
        from_block,
        process_transaction,
    );
    
    println!("Successfully subscribed to VIDA {} transactions", VIDA_ID);
    
    // Start monitoring loop for block progress
    tokio::spawn(async move {
        let mut last_checked = DatabaseService::get_last_checked_block().unwrap_or(0);
        
        loop {
            // Get current latest checked block from subscription
            let current_block = subscription.get_latest_checked_block();
            
            // If block has progressed, trigger validation
            if current_block > last_checked {
                on_chain_progress(current_block).await;
                last_checked = current_block;
            }
            
            sleep(Duration::from_secs(5)).await;
        }
    });
    
    println!("Block progress monitor started");
    Ok(())
}

/// Callback invoked as blocks are processed
async fn on_chain_progress(block_number: u64) {
    DatabaseService::set_last_checked_block(block_number).unwrap();
    check_root_hash_validity_and_save(block_number).await;
    println!("Checkpoint updated to block {}", block_number);
}

/// Processes a single VIDA transaction
fn process_transaction(txn: VidaDataTransaction) {
    let data_bytes = txn.data;
    
    // Parse JSON data
    let data_str = match String::from_utf8(data_bytes) {
        Ok(s) => s,
        Err(_) => {
            println!("Error decoding transaction data");
            return;
        }
    };
    
    let json_data: Value = match serde_json::from_str(&data_str) {
        Ok(json) => json,
        Err(_) => {
            println!("Error parsing transaction JSON");
            return;
        }
    };
    
    if let Some(obj_map) = json_data.as_object() {
        let action = obj_map.get("action")
            .and_then(|val| val.as_str())
            .unwrap_or("");
        
        if action.to_lowercase() == "transfer" {
            handle_transfer(obj_map, &txn.sender);
        }
    }
}

/// Executes a token transfer described by the given JSON payload
fn handle_transfer(json_data: &Map<String, Value>, sender_hex: &str) {
    // Extract amount and receiver from JSON
    let amount = match json_data.get("amount")
        .and_then(|val| {
            if let Some(s) = val.as_str() {
                s.parse::<BigUint>().ok()
            } else if let Some(n) = val.as_u64() {
                Some(BigUint::from(n))
            } else {
                None
            }
        }) {
        Some(amt) => amt,
        None => {
            println!("Invalid or missing amount");
            return;
        }
    };
    
    let receiver_hex = match json_data.get("receiver")
        .and_then(|val| val.as_str()) {
        Some(r) => r,
        None => {
            println!("Missing receiver");
            return;
        }
    };
    
    // Decode hex addresses
    let sender = decode_hex_address(sender_hex);
    let receiver = decode_hex_address(receiver_hex);
    
    // Execute transfer
    match DatabaseService::transfer(&sender, &receiver, &amount) {
        Ok(true) => {
            println!("Transfer succeeded: {} from {} to {}", amount, sender_hex, receiver_hex);
        }
        Ok(false) => {
            println!("Transfer failed (insufficient funds): {} from {} to {}", amount, sender_hex, receiver_hex);
        }
        Err(_) => {
            println!("Transfer operation failed");
        }
    }
}

/// Decodes a hexadecimal address into raw bytes
fn decode_hex_address(hex_str: &str) -> Vec<u8> {
    let clean_hex = if hex_str.starts_with("0x") {
        &hex_str[2..]
    } else {
        hex_str
    };
    hex::decode(clean_hex).unwrap_or_default()
}

/// Validates the local Merkle root against peers and persists it if a quorum of peers agree
async fn check_root_hash_validity_and_save(block_number: u64) {
    let local_root = match DatabaseService::get_root_hash() {
        Ok(Some(root)) => root,
        _ => {
            println!("No local root hash available for block {}", block_number);
            return;
        }
    };
    
    let peers = unsafe { &PEERS_TO_CHECK_ROOT_HASH_WITH };
    let mut peers_count = peers.len();
    let mut quorum = (peers_count * 2) / 3 + 1;
    let mut matches = 0;
    
    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    
    for peer in peers {
        let (success, peer_root) = fetch_peer_root_hash(&client, peer, block_number).await;
        
        if success && peer_root.is_some() {
            if peer_root.unwrap() == local_root {
                matches += 1;
            }
        } else {
            if peers_count > 0 {
                peers_count -= 1;
                quorum = (peers_count * 2) / 3 + 1;
            }
        }
        
        if matches >= quorum {
            DatabaseService::set_block_root_hash(block_number, &local_root).unwrap();
            println!("Root hash validated and saved for block {}", block_number);
            return;
        }
    }
    
    println!("Root hash mismatch: only {}/{} peers agreed", matches, peers.len());
    
    // Revert changes and reset block to reprocess the data
    DatabaseService::revert_unsaved_changes().unwrap();
}

/// Fetches the root hash from a peer node for the specified block number
async fn fetch_peer_root_hash(
    client: &reqwest::Client,
    peer: &str, 
    block_number: u64
) -> (bool, Option<Vec<u8>>) {
    let url = format!("http://{}/rootHash?blockNumber={}", peer, block_number);
    
    match client.get(&url)
        .header("Accept", "text/plain")
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(hex_string) => {
                        let trimmed = hex_string.trim();
                        if trimmed.is_empty() {
                            println!("Peer {} returned empty root hash for block {}", peer, block_number);
                            (false, None)
                        } else {
                            match hex::decode(trimmed) {
                                Ok(root_hash) => {
                                    println!("Successfully fetched root hash from peer {} for block {}", peer, block_number);
                                    (true, Some(root_hash))
                                }
                                Err(_) => {
                                    println!("Invalid hex response from peer {} for block {}", peer, block_number);
                                    (false, None)
                                }
                            }
                        }
                    }
                    Err(_) => {
                        println!("Failed to read response from peer {} for block {}", peer, block_number);
                        (false, None)
                    }
                }
            } else {
                println!("Peer {} returned HTTP {} for block {}", peer, response.status(), block_number);
                (true, None)
            }
        }
        Err(_) => {
            println!("Failed to fetch root hash from peer {} for block {}", peer, block_number);
            (false, None)
        }
    }
}

async fn shutdown() -> Result<(), Box<dyn std::error::Error>> {
    // Flush any pending database changes
    DatabaseService::flush().map_err(|e| format!("Failed to flush database: {:?}", e))?;
    println!("Flushed database changes");
    Ok(())
}

/// Application entry point for synchronizing VIDA transactions
/// with the local Merkle-backed database.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting PWR VIDA Transaction Synchronizer...");

    initialize_peers();
    DatabaseService::initialize().map_err(|e| format!("Database initialization failed: {:?}", e))?;

    start_api_server().await;
    init_initial_balances().await?;

    let last_block = DatabaseService::get_last_checked_block().map_err(|e| format!("Failed to get last checked block: {:?}", e))?;
    let from_block = if last_block > 0 { last_block } else { START_BLOCK };

    println!("Starting synchronization from block {}", from_block);

    subscribe_and_sync(from_block).await?;

    // Keep the main thread alive
    println!("Application started successfully. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?;

    // Graceful shutdown
    shutdown().await?;

    Ok(())
}
