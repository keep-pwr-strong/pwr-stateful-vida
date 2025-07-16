mod database_service;
mod api;
mod handler;

use std::env;
use std::time::Duration;
use hex;
use num_bigint::BigUint;
use tokio::time::sleep;

use crate::database_service::DatabaseService;
use crate::api::GET;
use crate::handler::{subscribe_and_sync, PEERS_TO_CHECK_ROOT_HASH_WITH};

// Constants
const START_BLOCK: u64 = 1;
const PORT: u16 = 8080;

// Initializes peer list from arguments or defaults
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

// Sets up the initial account balances when starting from a fresh database
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
        println!("Initial balances setup completed");
    }
    
    Ok(())
}

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

    Ok(())
}
