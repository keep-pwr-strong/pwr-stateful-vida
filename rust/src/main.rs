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
use std::collections::HashMap;
use tokio::time::sleep;

use crate::database_service::DatabaseService;
use crate::api::GET;

/// Main application struct that orchestrates the entire system.
/// Rust equivalent of the Python Main class.
#[allow(dead_code)]
pub struct Main {
    vida_id: u64,
    start_block: u64,
    rpc_url: String,
    default_port: u16,
    request_timeout_secs: u64,
    
    // Initial balances for fresh database
    initial_balances: HashMap<Vec<u8>, BigUint>,
    
    // Instance variables
    pwrpy_client: Option<Arc<RPC>>,
    peers_to_check_root_hash_with: Vec<String>,
    port: u16,
}

impl Main {
    /// Initialize the Main application
    pub fn new() -> Self {
        let mut initial_balances = HashMap::new();
        
        // Set up initial balances (equivalent to Python INITIAL_BALANCES)
        initial_balances.insert(
            hex::decode("c767ea1d613eefe0ce1610b18cb047881bafb829").unwrap(),
            BigUint::from(1_000_000_000_000u64)
        );
        initial_balances.insert(
            hex::decode("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4").unwrap(),
            BigUint::from(1_000_000_000_000u64)
        );
        initial_balances.insert(
            hex::decode("9282d39ca205806473f4fde5bac48ca6dfb9d300").unwrap(),
            BigUint::from(1_000_000_000_000u64)
        );
        initial_balances.insert(
            hex::decode("E68191B7913E72E6F1759531FBFAA089FF02308A").unwrap(),
            BigUint::from(1_000_000_000_000u64)
        );
        
        Self {
            vida_id: 73_746_238,
            start_block: 1,
            rpc_url: "https://pwrrpc.pwrlabs.io/".to_string(),
            default_port: 8080,
            request_timeout_secs: 10,
            initial_balances,
            pwrpy_client: None,
            peers_to_check_root_hash_with: Vec::new(),
            port: 8080,
        }
    }
    
    /// Parse command line arguments (equivalent to Python parse_command_line_args)
    fn parse_command_line_args(&mut self) {
        let args: Vec<String> = env::args().collect();
        
        // Parse port if provided (simple implementation)
        // In a real app, you'd use clap or similar
        if args.len() > 1 {
            if let Ok(port) = args[1].parse::<u16>() {
                self.port = port;
                println!("Using port from args: {}", self.port);
            }
        }
        
        // Parse peers (everything after port or from index 1)
        let peer_start = if args.len() > 1 && args[1].parse::<u16>().is_ok() { 2 } else { 1 };
        if args.len() > peer_start {
            self.peers_to_check_root_hash_with = args[peer_start..].to_vec();
        }
    }
    
    /// Sets up the initial account balances when starting from a fresh database.
    /// Equivalent to Python init_initial_balances() method.
    async fn init_initial_balances(&self) -> Result<(), Box<dyn std::error::Error>> {
        if DatabaseService::get_last_checked_block().map_err(|e| format!("Failed to get last checked block: {:?}", e))? == 0 {
            println!("Setting up initial balances for fresh database");
            
            for (address, balance) in &self.initial_balances {
                DatabaseService::set_balance(address, balance).map_err(|e| format!("Failed to set balance: {:?}", e))?;
                println!("Set initial balance for {}: {}", hex::encode(address), balance);
            }
            
            // Flush to ensure balances are persisted
            DatabaseService::flush().map_err(|e| format!("Failed to flush database: {:?}", e))?;
            println!("Initial balances setup completed");
        }
        
        Ok(())
    }
    
    /// Initializes peer list from arguments or defaults.
    /// Equivalent to Python initialize_peers() method.
    fn initialize_peers(&mut self) {
        if self.peers_to_check_root_hash_with.is_empty() {
            self.peers_to_check_root_hash_with = vec![
                "localhost:8080".to_string(),
            ];
            println!("Using default peers: {:?}", self.peers_to_check_root_hash_with);
        } else {
            println!("Using peers from args: {:?}", self.peers_to_check_root_hash_with);
        }
    }
    
    /// Start the API server in a background task
    /// Equivalent to Python start_flask_server() method.
    async fn start_api_server(&self) {
        let routes = GET::run();
        let port = self.port;
        
        tokio::spawn(async move {
            println!("Starting API server on port {}", port);
            warp::serve(routes)
                .run(([0, 0, 0, 0], port))
                .await;
        });
        
        // Give server time to start
        sleep(Duration::from_millis(2000)).await;
        println!("API server started on http://0.0.0.0:{}", self.port);
    }
    
    /// Subscribes to VIDA transactions starting from the given block.
    /// Equivalent to Python subscribe_and_sync() method.
    async fn subscribe_and_sync(&mut self, from_block: u64) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting VIDA transaction subscription from block {}", from_block);
        
        // Initialize RPC client
        let rpc = RPC::new(&self.rpc_url).await.map_err(|e| format!("Failed to create RPC client: {:?}", e))?;
        let rpc = Arc::new(rpc);
        self.pwrpy_client = Some(rpc.clone());
        
        // Subscribe to VIDA transactions using pwr_rs (like Python pwrpy)
        let subscription = rpc.subscribe_to_vida_transactions(
            self.vida_id,
            from_block,
            Self::process_transaction, // Transaction handler callback
        );
        
        println!("Successfully subscribed to VIDA {} transactions", self.vida_id);
        
        // Start monitoring loop for block progress (equivalent to Python _start_block_progress_monitor)
        let peers = self.peers_to_check_root_hash_with.clone();
        tokio::spawn(async move {
            let mut last_checked = DatabaseService::get_last_checked_block().unwrap_or(0);
            
            loop {
                // Get current latest checked block from subscription
                let current_block = subscription.get_latest_checked_block();
                
                // If block has progressed, trigger validation
                if current_block > last_checked {
                    if let Err(e) = Self::on_chain_progress(current_block, &peers).await {
                        eprintln!("Error in chain progress: {:?}", e);
                    }
                    last_checked = current_block;
                }
                
                sleep(Duration::from_secs(5)).await; // Check every 5 seconds
            }
        });
        
        println!("Block progress monitor started");
        Ok(())
    }
    
    /// Callback invoked as blocks are processed.
    /// Equivalent to Python on_chain_progress() method.
    async fn on_chain_progress(block_number: u64, peers: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        DatabaseService::set_last_checked_block(block_number).map_err(|e| format!("Failed to set last checked block: {:?}", e))?;
        Self::check_root_hash_validity_and_save(block_number, peers).await?;
        println!("Checkpoint updated to block {}", block_number);
        Ok(())
    }
    
    /// Processes a single VIDA transaction.
    /// Equivalent to Python process_transaction() method.
    fn process_transaction(txn: VidaDataTransaction) {
        println!("TRANSACTION RECEIVED: {}", hex::encode(&txn.data));
        
        match Self::handle_transaction(txn) {
            Ok(_) => {},
            Err(e) => {
                eprintln!("Error processing transaction: {:?}", e);
            }
        }
    }
    
    fn handle_transaction(txn: VidaDataTransaction) -> Result<(), Box<dyn std::error::Error>> {
        // Get transaction data and convert from hex to bytes (like Python)
        let data_bytes = txn.data; // pwr_rs should provide bytes directly
        
        // Parse JSON data
        let data_str = String::from_utf8(data_bytes)?;
        let json_data: Value = serde_json::from_str(&data_str)?;
        
        if let Some(obj_map) = json_data.as_object() {
            // Get action from JSON
            let action = obj_map.get("action")
                .and_then(|val| val.as_str())
                .unwrap_or("");
            
            if action.to_lowercase() == "transfer" {
                Self::handle_transfer(obj_map, &txn.sender)?;
            } else {
                println!("Ignoring transaction with action: {}", action);
            }
        }
        
        Ok(())
    }
    
    /// Executes a token transfer described by the given JSON payload.
    /// Equivalent to Python handle_transfer() method.
    fn handle_transfer(
        json_data: &Map<String, Value>, 
        sender_hex: &str
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Extract amount and receiver from JSON
        let amount = json_data.get("amount")
            .and_then(|val| {
                if let Some(s) = val.as_str() {
                    s.parse::<BigUint>().ok()
                } else if let Some(n) = val.as_u64() {
                    Some(BigUint::from(n))
                } else {
                    None
                }
            })
            .ok_or("Invalid or missing amount")?;
        
        let receiver_hex = json_data.get("receiver")
            .and_then(|val| val.as_str())
            .ok_or("Missing receiver")?;
        
        // Decode hex addresses
        let sender = Self::decode_hex_address(sender_hex)?;
        let receiver = Self::decode_hex_address(receiver_hex)?;
        
        // Execute transfer
        let success = DatabaseService::transfer(&sender, &receiver, &amount).map_err(|e| format!("Failed to execute transfer: {:?}", e))?;
        
        if success {
            println!("Transfer succeeded: {} from {} to {}", amount, sender_hex, receiver_hex);
        } else {
            println!("Transfer failed (insufficient funds): {:?}", json_data);
        }
        
        Ok(())
    }
    
    /// Decodes a hexadecimal address into raw bytes.
    /// Equivalent to Python decode_hex_address() method.
    fn decode_hex_address(hex_str: &str) -> Result<Vec<u8>, hex::FromHexError> {
        // Remove '0x' prefix if present
        let clean_hex = if hex_str.starts_with("0x") {
            &hex_str[2..]
        } else {
            hex_str
        };
        hex::decode(clean_hex)
    }
    
    /// Validates the local Merkle root against peers and persists it if a quorum
    /// of peers agree. Equivalent to Python check_root_hash_validity_and_save() method.
    async fn check_root_hash_validity_and_save(block_number: u64, peers: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        let local_root = match DatabaseService::get_root_hash().map_err(|e| format!("Failed to get root hash: {:?}", e))? {
            Some(root) => root,
            None => {
                println!("No local root hash available for block {}", block_number);
                return Ok(());
            }
        };
        
        let mut peers_count = peers.len();
        let mut quorum = (peers_count * 2) / 3 + 1;
        let mut matches = 0;
        
        // Create HTTP client
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        
        for peer in peers {
            let (success, peer_root) = Self::fetch_peer_root_hash(&client, peer, block_number).await;
            
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
                DatabaseService::set_block_root_hash(block_number, &local_root).map_err(|e| format!("Failed to set block root hash: {:?}", e))?;
                println!("Root hash validated and saved for block {}", block_number);
                return Ok(());
            }
        }
        
        eprintln!("Root hash mismatch: only {}/{} peers agreed", matches, peers.len());
        
        // Revert changes and reset block to reprocess the data
        DatabaseService::revert_unsaved_changes().map_err(|e| format!("Failed to revert unsaved changes: {:?}", e))?;
        // Note: In real implementation, you'd reset the subscription
        
        Ok(())
    }
    
    /// Fetches the root hash from a peer node for the specified block number.
    /// Equivalent to Python fetch_peer_root_hash() method.
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
                                    Err(e) => {
                                        println!("Invalid hex response from peer {} for block {}: {:?}", peer, block_number, e);
                                        (false, None)
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("Failed to read response from peer {} for block {}: {:?}", peer, block_number, e);
                            (false, None)
                        }
                    }
                } else {
                    println!("Peer {} returned HTTP {} for block {}", peer, response.status(), block_number);
                    (true, None) // Peer responded but with error
                }
            }
            Err(e) => {
                println!("Failed to fetch root hash from peer {} for block {}: {:?}", peer, block_number, e);
                (false, None)
            }
        }
    }
    
    /// Main application entry point that orchestrates the entire system.
    /// Equivalent to Python run() method.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("=== Starting PWR VIDA Synchronizer ===");
        
        // Parse command line arguments
        self.parse_command_line_args();
        
        // Initialize database service
        DatabaseService::initialize().map_err(|e| format!("Database initialization failed: {:?}", e))?;
        
        // Start API server
        self.start_api_server().await;
        
        // Initialize initial balances
        self.init_initial_balances().await?;
        
        // Initialize peers
        self.initialize_peers();
        
        // Determine starting block
        let last_block = DatabaseService::get_last_checked_block().map_err(|e| format!("Failed to get last checked block: {:?}", e))?;
        let from_block = if last_block > 0 { last_block } else { self.start_block };
        
        println!("Starting synchronization from block {}", from_block);
        
        // Subscribe and sync
        self.subscribe_and_sync(from_block).await?;
        
        // Keep the main thread alive
        println!("Application started successfully. Press Ctrl+C to exit.");
        tokio::signal::ctrl_c().await?;
        
        // Graceful shutdown
        self.shutdown().await?;
        
        Ok(())
    }
    
    /// Graceful application shutdown
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Shutting down application...");
        
        // Flush any pending database changes
        DatabaseService::flush().map_err(|e| format!("Failed to flush database: {:?}", e))?;
        println!("Flushed database changes");
        
        println!("Application shutdown complete");
        Ok(())
    }
}

/// Application entry point.
/// Equivalent to Python main() function
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Main::new();
    app.run().await
}
