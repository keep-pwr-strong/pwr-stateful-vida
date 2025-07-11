"""
DatabaseService: Python conversion of Java DatabaseService

Singleton service for interacting with the underlying MerkleTree.
Provides methods for managing account balances, transfers, block tracking, and
Merkle root hash operations.
"""

import atexit
import struct
import threading
from typing import Optional
from pwrpy.models.MerkleTree import MerkleTree

class DatabaseServiceError(Exception):
    """Custom exception for DatabaseService operations"""
    pass

# Global instance and lock
_tree = None
_lock = threading.Lock()

# Constants
LAST_CHECKED_BLOCK_KEY = b"lastCheckedBlock"
BLOCK_ROOT_PREFIX = "blockRootHash_"

def _get_tree():
    """Get or create the global MerkleTree instance"""
    global _tree
    if _tree is None:
        with _lock:
            if _tree is None:
                try:
                    _tree = MerkleTree("database")
                    atexit.register(_shutdown_hook)
                except Exception as e:
                    raise DatabaseServiceError(f"Failed to initialize MerkleTree: {str(e)}")
    return _tree

def _shutdown_hook():
    """Cleanup method called on application shutdown"""
    global _tree
    try:
        if _tree is not None:
            _tree.close()
    except Exception as e:
        print(f"Error during DatabaseService shutdown: {e}")

def get_root_hash() -> Optional[bytes]:
    """Get current Merkle root hash"""
    try:
        tree = _get_tree()
        return tree.get_root_hash()
    except Exception as e:
        raise DatabaseServiceError(f"Error getting root hash: {str(e)}")

def flush():
    """Flush pending writes to disk"""
    try:
        tree = _get_tree()
        tree.flush_to_disk()
    except Exception as e:
        raise DatabaseServiceError(f"Error flushing to disk: {str(e)}")

def revert_unsaved_changes():
    """Reverts all unsaved changes to the Merkle tree"""
    try:
        tree = _get_tree()
        tree.revert_unsaved_changes()
    except Exception as e:
        raise DatabaseServiceError(f"Error reverting changes: {str(e)}")

def get_balance(address: bytes) -> int:
    """Retrieves the balance stored at the given address"""
    if address is None:
        raise ValueError("Address must not be null")
    
    try:
        tree = _get_tree()
        data = tree.get_data(address)
        
        if data is None or len(data) == 0:
            return 0
        
        return int.from_bytes(data, byteorder='big', signed=False)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error getting balance: {str(e)}")

def set_balance(address: bytes, balance: int):
    """Sets the balance for the given address"""
    if address is None:
        raise ValueError("Address must not be null")
    if balance is None:
        raise ValueError("Balance must not be null")
    if balance < 0:
        raise ValueError("Balance must be non-negative")
    
    try:
        tree = _get_tree()
        balance_bytes = balance.to_bytes((balance.bit_length() + 7) // 8, byteorder='big', signed=False)
        if balance == 0:
            balance_bytes = b'\x00'
        
        tree.add_or_update_data(address, balance_bytes)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error setting balance: {str(e)}")

def transfer(sender: bytes, receiver: bytes, amount: int) -> bool:
    """Transfers amount from sender to receiver"""
    if sender is None:
        raise ValueError("Sender must not be null")
    if receiver is None:
        raise ValueError("Receiver must not be null")
    if amount is None:
        raise ValueError("Amount must not be null")
    
    try:
        sender_balance = get_balance(sender)
        if sender_balance < amount:
            return False
        
        set_balance(sender, sender_balance - amount)
        receiver_balance = get_balance(receiver)
        set_balance(receiver, receiver_balance + amount)
        
        return True
        
    except Exception as e:
        raise DatabaseServiceError(f"Error during transfer: {str(e)}")

def get_last_checked_block() -> int:
    """Get the last checked block number"""
    try:
        tree = _get_tree()
        data = tree.get_data(LAST_CHECKED_BLOCK_KEY)
        
        if data is None or len(data) < 8:
            return 0
        
        return struct.unpack('>Q', data)[0]
        
    except Exception as e:
        raise DatabaseServiceError(f"Error getting last checked block: {str(e)}")

def set_last_checked_block(block_number: int):
    """Updates the last checked block number"""
    if block_number < 0:
        raise ValueError("Block number must be non-negative")
    
    try:
        tree = _get_tree()
        data = struct.pack('>Q', block_number)
        tree.add_or_update_data(LAST_CHECKED_BLOCK_KEY, data)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error setting last checked block: {str(e)}")

def set_block_root_hash(block_number: int, root_hash: bytes):
    """Records the Merkle root hash for a specific block"""
    if root_hash is None:
        raise ValueError("Root hash must not be null")
    
    try:
        tree = _get_tree()
        key = f"{BLOCK_ROOT_PREFIX}{block_number}"
        tree.add_or_update_data(key.encode('utf-8'), root_hash)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error setting block root hash: {str(e)}")

def get_block_root_hash(block_number: int) -> Optional[bytes]:
    """Retrieves the Merkle root hash for a specific block"""
    try:
        tree = _get_tree()
        key = f"{BLOCK_ROOT_PREFIX}{block_number}"
        return tree.get_data(key.encode('utf-8'))
        
    except Exception as e:
        raise DatabaseServiceError(f"Error getting block root hash: {str(e)}")

