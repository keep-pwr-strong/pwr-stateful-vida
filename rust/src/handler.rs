use pwr_rs::{
    RPC,
    transaction::types::VidaDataTransaction,
    rpc::types::VidaTransactionSubscription,
};
use std::sync::Arc;
use std::time::Duration;
use hex;
use serde_json::{Value, Map};
use num_bigint::BigUint;
use tokio::time::sleep;

use crate::database_service::DatabaseService;

// Constants
const VIDA_ID: u64 = 73_746_238;
const RPC_URL: &str = "https://pwrrpc.pwrlabs.io/";

// Global state
static mut subscription: Option<VidaTransactionSubscription> = None;

// Fetches the root hash from a peer node for the specified block number
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

// Validates the local Merkle root against peers and persists it if a quorum of peers agree
async fn check_root_hash_validity_and_save(block_number: u64, peers: Vec<String>) {
    let local_root = match DatabaseService::get_root_hash() {
        Ok(Some(root)) => root,
        _ => {
            println!("No local root hash available for block {}", block_number);
            return;
        }
    };
    
    let peers = unsafe { &peers };
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

// Executes a token transfer described by the given JSON payload
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
    let sender_address = if sender_hex.starts_with("0x") { &sender_hex[2..] } else { sender_hex };
    let receiver_address = if receiver_hex.starts_with("0x") { &receiver_hex[2..] } else { receiver_hex };

    let sender = hex::decode(sender_address).unwrap_or_default();
    let receiver = hex::decode(receiver_address).unwrap_or_default();
    
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

// Processes a single VIDA transaction
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

// Callback invoked as blocks are processed
async fn on_chain_progress(block_number: u64, peers: Vec<String>) {
    DatabaseService::set_last_checked_block(block_number).unwrap();
    check_root_hash_validity_and_save(block_number, peers).await;
    println!("Checkpoint updated to block {}", block_number);
    DatabaseService::flush().map_err(|e| format!("Failed to flush database: {:?}", e)).unwrap();
}

// Subscribes to VIDA transactions starting from the given block
pub async fn subscribe_and_sync(from_block: u64, peers: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting VIDA transaction subscription from block {}", from_block);
    
    // Initialize RPC client
    let rpc = RPC::new(RPC_URL).await.map_err(|e| format!("Failed to create RPC client: {:?}", e))?;
    let rpc = Arc::new(rpc);
    
    // Subscribe to VIDA transactions
    unsafe {
        subscription = Some(rpc.subscribe_to_vida_transactions(
            VIDA_ID,
            from_block,
            process_transaction,
        ));
    }
    
    println!("Successfully subscribed to VIDA {} transactions", VIDA_ID);
    
    // Start monitoring loop for block progress
    tokio::spawn(async move {
        let mut last_checked = DatabaseService::get_last_checked_block().unwrap_or(0);

        loop {
            // Get current latest checked block from subscription
            let current_block = unsafe { subscription.as_ref().unwrap().get_latest_checked_block() };

            // If block has progressed, trigger validation
            if current_block > last_checked {
                on_chain_progress(current_block, peers.clone()).await;
                last_checked = current_block;
            }

            sleep(Duration::from_secs(5)).await;
        }
    });

    println!("Block progress monitor started");
    Ok(())
}
