use std::sync::{Arc, OnceLock};
use pwr_rs::merkle_tree::{MerkleTree, MerkleTreeError};
use num_bigint::BigUint;
use std::convert::TryInto;

/// Singleton service for interacting with the underlying RocksDB-backed MerkleTree.
/// Provides methods for managing account balances, transfers, block tracking, and
/// Merkle root hash operations.
pub struct DatabaseService;

// Global static instance of the MerkleTree
static TREE: OnceLock<Arc<MerkleTree>> = OnceLock::new();

// Constants
const LAST_CHECKED_BLOCK_KEY: &[u8] = b"lastCheckedBlock";
const BLOCK_ROOT_PREFIX: &str = "blockRootHash_";

impl DatabaseService {
    /// Initialize the DatabaseService. Must be called once before using any other methods.
    pub fn initialize() -> Result<(), MerkleTreeError> {
        let tree = MerkleTree::new("database".to_string())?;
        TREE.set(tree).map_err(|_| {
            MerkleTreeError::IllegalState("DatabaseService already initialized".to_string())
        })?;
        Ok(())
    }
    
    /// Get the global tree instance
    fn get_tree() -> Result<&'static Arc<MerkleTree>, MerkleTreeError> {
        TREE.get().ok_or_else(|| {
            MerkleTreeError::IllegalState("DatabaseService not initialized. Call initialize() first.".to_string())
        })
    }
    
    /// Get current Merkle root hash
    pub fn get_root_hash() -> Result<Option<Vec<u8>>, MerkleTreeError> {
        let tree = Self::get_tree()?;
        tree.get_root_hash()
    }
    
    /// Flush pending writes to disk
    pub fn flush() -> Result<(), MerkleTreeError> {
        let tree = Self::get_tree()?;
        tree.flush_to_disk()
    }
    
    /// Reverts all unsaved changes to the Merkle tree
    pub fn revert_unsaved_changes() -> Result<(), MerkleTreeError> {
        let tree = Self::get_tree()?;
        tree.revert_unsaved_changes()
    }
    
    /// Retrieves the balance stored at the given address
    pub fn get_balance(address: &[u8]) -> Result<BigUint, MerkleTreeError> {
        if address.is_empty() {
            return Err(MerkleTreeError::InvalidArgument("Address must not be empty".to_string()));
        }
        
        let tree = Self::get_tree()?;
        let data = tree.get_data(address)?;
        
        match data {
            Some(bytes) if !bytes.is_empty() => {
                Ok(BigUint::from_bytes_be(&bytes))
            }
            _ => Ok(BigUint::from(0u32))
        }
    }
    
    /// Sets the balance for the given address
    pub fn set_balance(address: &[u8], balance: &BigUint) -> Result<(), MerkleTreeError> {
        if address.is_empty() {
            return Err(MerkleTreeError::InvalidArgument("Address must not be empty".to_string()));
        }
        
        let tree = Self::get_tree()?;
        let balance_bytes = balance.to_bytes_be();
        tree.add_or_update_data(address, &balance_bytes)
    }
    
    /// Transfers amount from sender to receiver
    pub fn transfer(sender: &[u8], receiver: &[u8], amount: &BigUint) -> Result<bool, MerkleTreeError> {
        if sender.is_empty() {
            return Err(MerkleTreeError::InvalidArgument("Sender address must not be empty".to_string()));
        }
        if receiver.is_empty() {
            return Err(MerkleTreeError::InvalidArgument("Receiver address must not be empty".to_string()));
        }
        
        let sender_balance = Self::get_balance(sender)?;
        
        if sender_balance < *amount {
            return Ok(false);
        }
        
        let new_sender_balance = &sender_balance - amount;
        let receiver_balance = Self::get_balance(receiver)?;
        let new_receiver_balance = &receiver_balance + amount;
        
        Self::set_balance(sender, &new_sender_balance)?;
        Self::set_balance(receiver, &new_receiver_balance)?;
        
        Ok(true)
    }
    
    /// Get the last checked block number
    pub fn get_last_checked_block() -> Result<u64, MerkleTreeError> {
        let tree = Self::get_tree()?;
        let data = tree.get_data(LAST_CHECKED_BLOCK_KEY)?;
        
        match data {
            Some(bytes) if bytes.len() >= 8 => {
                let block_bytes: [u8; 8] = bytes[..8].try_into()
                    .map_err(|_| MerkleTreeError::InvalidArgument("Invalid block number format".to_string()))?;
                Ok(u64::from_be_bytes(block_bytes))
            }
            _ => Ok(0)
        }
    }
    
    /// Updates the last checked block number
    pub fn set_last_checked_block(block_number: u64) -> Result<(), MerkleTreeError> {
        let tree = Self::get_tree()?;
        let block_bytes = block_number.to_be_bytes();
        tree.add_or_update_data(LAST_CHECKED_BLOCK_KEY, &block_bytes)
    }
    
    /// Records the Merkle root hash for a specific block
    pub fn set_block_root_hash(block_number: u64, root_hash: &[u8]) -> Result<(), MerkleTreeError> {
        if root_hash.is_empty() {
            return Err(MerkleTreeError::InvalidArgument("Root hash must not be empty".to_string()));
        }
        
        let tree = Self::get_tree()?;
        let key = format!("{}{}", BLOCK_ROOT_PREFIX, block_number);
        tree.add_or_update_data(key.as_bytes(), root_hash)
    }
    
    /// Retrieves the Merkle root hash for a specific block
    pub fn get_block_root_hash(block_number: u64) -> Result<Option<Vec<u8>>, MerkleTreeError> {
        let tree = Self::get_tree()?;
        let key = format!("{}{}", BLOCK_ROOT_PREFIX, block_number);
        tree.get_data(key.as_bytes())
    }
}
