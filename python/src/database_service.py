"""
DatabaseService: Python conversion of Java DatabaseService

Singleton service for interacting with the underlying MerkleTree.
Provides methods for managing account balances, transfers, block tracking, and
Merkle root hash operations.

This service maintains:
- Account balances stored in the Merkle tree
- Last checked block number for synchronization
- Historical block root hashes for validation

The underlying MerkleTree is automatically closed on application shutdown.
"""

import atexit
import struct
import threading
from typing import Optional
import sys
import os
from pwrpy.models.MerkleTree import MerkleTree


class DatabaseServiceError(Exception):
    """Custom exception for DatabaseService operations"""
    pass


class DatabaseService:
    """
    Singleton service for interacting with the underlying MerkleTree.
    Python equivalent of the Java DatabaseService class.
    """
    
    # Singleton instance
    _instance: Optional['DatabaseService'] = None
    _lock = threading.Lock()
    
    # Constants (equivalent to Java static final fields)
    LAST_CHECKED_BLOCK_KEY = b"lastCheckedBlock"
    BLOCK_ROOT_PREFIX = "blockRootHash_"
    
    def __new__(cls):
        """Ensure singleton pattern - only one instance can exist"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the DatabaseService with MerkleTree"""
        # Prevent re-initialization
        if hasattr(self, '_initialized'):
            return
        
        try:
            self._tree = MerkleTree("database")
            self._initialized = True
            
            # Register shutdown hook (equivalent to Java Runtime.addShutdownHook)
            atexit.register(self._shutdown_hook)
            
        except Exception as e:
            raise DatabaseServiceError(f"Failed to initialize MerkleTree: {str(e)}")
    
    def _shutdown_hook(self):
        """Cleanup method called on application shutdown"""
        try:
            if hasattr(self, '_tree') and self._tree is not None:
                self._tree.close()
        except Exception as e:
            print(f"Error during DatabaseService shutdown: {e}")
    
    @classmethod
    def get_instance(cls) -> 'DatabaseService':
        """Get the singleton instance of DatabaseService"""
        return cls()


# Module-level functions to mimic Java static methods
def get_root_hash() -> Optional[bytes]:
    """
    Get current Merkle root hash
    
    Returns:
        Current Merkle root hash or None if not available
        
    Raises:
        DatabaseServiceError: On database errors
    """
    try:
        service = DatabaseService.get_instance()
        return service._tree.get_root_hash()
    except Exception as e:
        raise DatabaseServiceError(f"Error getting root hash: {str(e)}")


def flush():
    """
    Flush pending writes to disk
    
    Raises:
        DatabaseServiceError: On database errors
    """
    try:
        service = DatabaseService.get_instance()
        service._tree.flush_to_disk()
    except Exception as e:
        raise DatabaseServiceError(f"Error flushing to disk: {str(e)}")


def revert_unsaved_changes():
    """
    Reverts all unsaved changes to the Merkle tree, restoring it to the last
    flushed state. This is useful for rolling back invalid transactions or
    when consensus validation fails.
    """
    try:
        service = DatabaseService.get_instance()
        service._tree.revert_unsaved_changes()
    except Exception as e:
        raise DatabaseServiceError(f"Error reverting changes: {str(e)}")


def get_balance(address: bytes) -> int:
    """
    Retrieves the balance stored at the given address.
    
    Args:
        address: Account address bytes
        
    Returns:
        Non-negative balance, zero if absent
        
    Raises:
        DatabaseServiceError: On database errors
        ValueError: If address is None
    """
    if address is None:
        raise ValueError("Address must not be null")
    
    try:
        service = DatabaseService.get_instance()
        data = service._tree.get_data(address)
        
        if data is None or len(data) == 0:
            return 0
        
        # Convert bytes back to integer (equivalent to Java BigInteger)
        # Python int has arbitrary precision like Java BigInteger
        return int.from_bytes(data, byteorder='big', signed=False)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error getting balance: {str(e)}")


def set_balance(address: bytes, balance: int):
    """
    Sets the balance for the given address.
    
    Args:
        address: Account address bytes
        balance: Non-negative balance
        
    Raises:
        DatabaseServiceError: On database errors
        ValueError: If address or balance is None, or balance is negative
    """
    if address is None:
        raise ValueError("Address must not be null")
    if balance is None:
        raise ValueError("Balance must not be null")
    if balance < 0:
        raise ValueError("Balance must be non-negative")
    
    try:
        service = DatabaseService.get_instance()
        # Convert integer to bytes (equivalent to Java BigInteger.toByteArray())
        balance_bytes = balance.to_bytes((balance.bit_length() + 7) // 8, byteorder='big', signed=False)
        if balance == 0:
            balance_bytes = b'\x00'  # Handle zero case
        
        service._tree.add_or_update_data(address, balance_bytes)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error setting balance: {str(e)}")


def transfer(sender: bytes, receiver: bytes, amount: int) -> bool:
    """
    Transfers amount from sender to receiver.
    
    Args:
        sender: Sender address
        receiver: Receiver address
        amount: Amount to transfer
        
    Returns:
        True if transfer succeeded, False on insufficient funds
        
    Raises:
        DatabaseServiceError: On database errors
        ValueError: If any parameter is None
    """
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
        
        # Perform the transfer
        set_balance(sender, sender_balance - amount)
        receiver_balance = get_balance(receiver)
        set_balance(receiver, receiver_balance + amount)
        
        return True
        
    except Exception as e:
        raise DatabaseServiceError(f"Error during transfer: {str(e)}")


def get_last_checked_block() -> int:
    """
    Get the last checked block number, or zero if unset
    
    Returns:
        Last checked block number
        
    Raises:
        DatabaseServiceError: On database errors
    """
    try:
        service = DatabaseService.get_instance()
        data = service._tree.get_data(DatabaseService.LAST_CHECKED_BLOCK_KEY)
        
        if data is None or len(data) < 8:  # 8 bytes for long
            return 0
        
        # Unpack long from bytes (equivalent to Java ByteBuffer.wrap(bytes).getLong())
        return struct.unpack('>Q', data)[0]  # Big-endian unsigned long
        
    except Exception as e:
        raise DatabaseServiceError(f"Error getting last checked block: {str(e)}")


def set_last_checked_block(block_number: int):
    """
    Updates the last checked block number.
    
    Args:
        block_number: Non-negative block number
        
    Raises:
        DatabaseServiceError: On database errors
        ValueError: If block_number is negative
    """
    if block_number < 0:
        raise ValueError("Block number must be non-negative")
    
    try:
        service = DatabaseService.get_instance()
        # Pack long to bytes (equivalent to Java ByteBuffer.allocate(Long.BYTES).putLong(blockNumber).array())
        data = struct.pack('>Q', block_number)  # Big-endian unsigned long
        service._tree.add_or_update_data(DatabaseService.LAST_CHECKED_BLOCK_KEY, data)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error setting last checked block: {str(e)}")


def set_block_root_hash(block_number: int, root_hash: bytes):
    """
    Records the Merkle root hash for a specific block.
    
    Args:
        block_number: Block height
        root_hash: 32-byte Merkle root
        
    Raises:
        DatabaseServiceError: On database errors
        ValueError: If root_hash is None
    """
    if root_hash is None:
        raise ValueError("Root hash must not be null")
    
    try:
        service = DatabaseService.get_instance()
        key = f"{DatabaseService.BLOCK_ROOT_PREFIX}{block_number}"
        key_bytes = key.encode('utf-8')
        service._tree.add_or_update_data(key_bytes, root_hash)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error setting block root hash: {str(e)}")


def get_block_root_hash(block_number: int) -> Optional[bytes]:
    """
    Retrieves the Merkle root hash for a specific block.
    
    Args:
        block_number: Block height
        
    Returns:
        32-byte root hash, or None if absent
        
    Raises:
        DatabaseServiceError: On database errors
    """
    try:
        service = DatabaseService.get_instance()
        key = f"{DatabaseService.BLOCK_ROOT_PREFIX}{block_number}"
        key_bytes = key.encode('utf-8')
        return service._tree.get_data(key_bytes)
        
    except Exception as e:
        raise DatabaseServiceError(f"Error getting block root hash: {str(e)}")

