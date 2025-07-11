use warp::Filter;
use std::collections::HashMap;
use crate::database_service::DatabaseService;

pub struct GET;

impl GET {
    /// Initializes and registers all GET endpoint handlers with the Warp framework.
    /// Currently registers the /rootHash endpoint for retrieving Merkle root hashes
    /// for specific block numbers.
    pub fn run() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path("rootHash")
            .and(warp::get())
            .and(warp::query::<HashMap<String, String>>())
            .map(|params: HashMap<String, String>| {
                match Self::handle_root_hash(params) {
                    Ok(response) => response,
                    Err(_) => String::new()
                }
            })
    }
    
    fn handle_root_hash(params: HashMap<String, String>) -> Result<String, String> {
        let block_number_str = params.get("blockNumber")
            .ok_or("Missing blockNumber parameter")?;
        let block_number: u64 = block_number_str.parse()
            .map_err(|_| "Invalid block number format")?;
        
        let last_checked_block = DatabaseService::get_last_checked_block()
            .map_err(|_| "Database error")?;
        
        if block_number == last_checked_block {
            let root_hash = DatabaseService::get_root_hash()
                .map_err(|_| "Database error")?;
            match root_hash {
                Some(hash) => Ok(hex::encode(hash)),
                None => Ok(String::new())
            }
        } else if block_number < last_checked_block && block_number > 1 {
            let block_root_hash = DatabaseService::get_block_root_hash(block_number)
                .map_err(|_| "Database error")?;
            
            match block_root_hash {
                Some(hash) => Ok(hex::encode(hash)),
                None => Ok(format!("Block root hash not found for block number: {}", block_number))
            }
        } else {
            Ok("Invalid block number".to_string())
        }
    }
}
