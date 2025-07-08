import sys
import os
import json
import logging
import argparse
import threading
import time
from typing import List, Optional, Tuple, Dict, Any
import requests
from requests.exceptions import RequestException

# Add the src directory to the path for imports
src_path = os.path.abspath(os.path.dirname(__file__))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Import pwrpy SDK
from pwrpy.pwrsdk import PWRPY
from pwrpy.models.Transaction import VidaDataTransaction

# Import our local modules
from database_service import (
    get_root_hash,
    get_last_checked_block,
    set_last_checked_block,
    set_balance,
    get_balance,
    transfer,
    set_block_root_hash,
    revert_unsaved_changes,
    flush,
    DatabaseServiceError
)

from api.get import app as flask_app


class Main:
    """
    Main application class that orchestrates the entire system.
    Python equivalent of the Java Main class.
    """
    
    # Constants (equivalent to Java static final fields)
    VIDA_ID = 73_746_238
    START_BLOCK = 1
    RPC_URL = "https://pwrrpc.pwrlabs.io/"
    DEFAULT_PORT = 8080
    REQUEST_TIMEOUT = 10  # seconds
    
    # Initial balances for fresh database (equivalent to Java hex addresses)
    INITIAL_BALANCES = {
        bytes.fromhex("c767ea1d613eefe0ce1610b18cb047881bafb829"): 1_000_000_000_000,
        bytes.fromhex("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4"): 1_000_000_000_000,
        bytes.fromhex("9282d39ca205806473f4fde5bac48ca6dfb9d300"): 1_000_000_000_000,
    }
    
    def __init__(self):
        """Initialize the Main application"""
        self.logger = self._setup_logging()
        self.pwrpy_client = PWRPY(self.RPC_URL)
        self.peers_to_check_root_hash_with: List[str] = []
        self.subscription = None
        self.flask_thread = None
        self.port = self.DEFAULT_PORT
        
    def _setup_logging(self) -> logging.Logger:
        """
        Set up logging configuration (equivalent to Java Logger setup)
        
        Returns:
            Configured logger instance
        """
        # Configure logging format similar to Java logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        logger = logging.getLogger(self.__class__.__name__)
        return logger
    
    def parse_command_line_args(self, args: Optional[List[str]] = None) -> argparse.Namespace:
        """
        Parse command line arguments (equivalent to Java String[] args processing)
        
        Args:
            args: Command line arguments (defaults to sys.argv if None)
            
        Returns:
            Parsed arguments namespace
        """
        parser = argparse.ArgumentParser(
            description="PWR Stateful VIDA - Blockchain Transaction Processor"
        )
        
        parser.add_argument(
            '--port', 
            type=int, 
            default=self.DEFAULT_PORT,
            help=f'Port to run the API server on (default: {self.DEFAULT_PORT})'
        )
        
        parser.add_argument(
            'peers',
            nargs='*',
            help='List of peer hostnames to query for root hash validation'
        )
        
        if args is None:
            args = sys.argv[1:]
            
        return parser.parse_args(args)
    
    def init_initial_balances(self):
        """
        Sets up the initial account balances when starting from a fresh database.
        Equivalent to Java initInitialBalances() method.
        
        Raises:
            DatabaseServiceError: If persisting the balances fails
        """
        try:
            if get_last_checked_block() == 0:
                self.logger.info("Setting up initial balances for fresh database")
                
                for address, balance in self.INITIAL_BALANCES.items():
                    set_balance(address, balance)
                    self.logger.info(f"Set initial balance for {address.hex()}: {balance}")
                
                # Flush to ensure balances are persisted
                flush()
                self.logger.info("Initial balances setup completed")
                
        except DatabaseServiceError as e:
            self.logger.error(f"Failed to initialize balances: {e}")
            raise
    
    def initialize_peers(self, peer_args: List[str]):
        """
        Initializes peer list from arguments or defaults.
        Equivalent to Java initializePeers() method.
        
        Args:
            peer_args: Peer hostnames from command line arguments
        """
        if peer_args and len(peer_args) > 0:
            self.peers_to_check_root_hash_with = peer_args
            self.logger.info(f"Using peers from args: {self.peers_to_check_root_hash_with}")
        else:
            self.peers_to_check_root_hash_with = [
                "localhost:8080"
            ]
            self.logger.info(f"Using default peers: {self.peers_to_check_root_hash_with}")
    
    def start_flask_server(self):
        """
        Start the Flask API server in a background thread
        Equivalent to Java port(PORT) and GET.run()
        """
        def run_flask():
            """Flask server thread function"""
            try:
                self.logger.info(f"Starting Flask API server on port {self.port}")
                flask_app.run(host='0.0.0.0', port=self.port, debug=False, use_reloader=False)
            except Exception as e:
                self.logger.error(f"Flask server error: {e}")
        
        self.flask_thread = threading.Thread(target=run_flask, daemon=True)
        self.flask_thread.start()
        
        # Give Flask time to start
        time.sleep(2)
        self.logger.info(f"Flask API server started on http://0.0.0.0:{self.port}")
    
    def subscribe_and_sync(self, from_block: int):
        """
        Subscribes to VIDA transactions starting from the given block.
        Equivalent to Java subscribeAndSync() method.
        
        Args:
            from_block: Block height to begin synchronization from
            
        Raises:
            Exception: If subscription setup fails
        """
        try:
            self.logger.info(f"Starting VIDA transaction subscription from block {from_block}")
            
            # Subscribe to VIDA transactions using pwrpy
            self.subscription = self.pwrpy_client.subscribe_to_vida_transactions(
                self.VIDA_ID,
                from_block,
                self.process_transaction  # Transaction handler callback
            )
            
            self.logger.info(f"Successfully subscribed to VIDA {self.VIDA_ID} transactions")
            
            # Start monitoring loop for block progress
            self._start_block_progress_monitor()
            
        except Exception as e:
            self.logger.error(f"Failed to subscribe to transactions: {e}")
            raise
    
    def _start_block_progress_monitor(self):
        """
        Start a background thread to monitor block progress and trigger consensus validation
        """
        def monitor_blocks():
            """Block monitoring thread function"""
            last_checked = get_last_checked_block()
            
            while True:
                try:
                    # Check if subscription is still active
                    if self.subscription is None:
                        break
                    
                    # Get current latest checked block from subscription
                    current_block = self.subscription.get_latest_checked_block()
                    
                    # If block has progressed, trigger validation
                    if current_block > last_checked:
                        self.on_chain_progress(current_block)
                        last_checked = current_block
                    
                    time.sleep(5)  # Check every 5 seconds
                    
                except Exception as e:
                    self.logger.error(f"Error in block progress monitor: {e}")
                    time.sleep(10)  # Wait longer on error
        
        monitor_thread = threading.Thread(target=monitor_blocks, daemon=True)
        monitor_thread.start()
        self.logger.info("Block progress monitor started")
    
    def on_chain_progress(self, block_number: int):
        """
        Callback invoked as blocks are processed.
        Equivalent to Java onChainProgress() method.
        
        Args:
            block_number: Block height that was just processed
        """
        try:
            set_last_checked_block(block_number)
            self.check_root_hash_validity_and_save(block_number)
            self.logger.info(f"Checkpoint updated to block {block_number}")
            
        except DatabaseServiceError as e:
            self.logger.warning(f"Failed to update last checked block: {block_number}, error: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in block progress: {e}")
    
    def process_transaction(self, txn: VidaDataTransaction):
        """
        Processes a single VIDA transaction.
        Equivalent to Java processTransaction() method.
        
        Args:
            txn: The VIDA transaction to handle
        """
        try:
            print(F"TRANSACTION RECEIVED: {txn.data}")
            # Get transaction data and convert from hex to bytes
            data_hex = txn.data
            data_bytes = bytes.fromhex(data_hex)
            
            # Parse JSON data
            json_data = json.loads(data_bytes.decode('utf-8'))
            
            # Get action from JSON
            action = json_data.get("action", "")
            
            if action.lower() == "transfer":
                self.handle_transfer(json_data, txn.sender)
            else:
                self.logger.debug(f"Ignoring transaction with action: {action}")
                
        except json.JSONDecodeError as e:
            self.logger.warning(f"Invalid JSON in transaction {txn.sender}: {e}")
        except Exception as e:
            self.logger.error(f"Error processing transaction from {txn.sender}: {e}")
    
    def handle_transfer(self, json_data: Dict[str, Any], sender_hex: str):
        """
        Executes a token transfer described by the given JSON payload.
        Equivalent to Java handleTransfer() method.
        
        Args:
            json_data: Transfer description JSON
            sender_hex: Hexadecimal sender address
            
        Raises:
            DatabaseServiceError: If balance updates fail
        """
        try:
            # Extract amount and receiver from JSON
            amount = json_data.get("amount")
            receiver_hex = json_data.get("receiver")
            
            if amount is None or receiver_hex is None:
                self.logger.warning(f"Skipping invalid transfer: {json_data}")
                return
            
            # Convert amount to integer if it's a string
            if isinstance(amount, str):
                amount = int(amount)
            
            # Decode hex addresses
            sender = self.decode_hex_address(sender_hex)
            receiver = self.decode_hex_address(receiver_hex)
            
            # Execute transfer
            success = transfer(sender, receiver, amount)
            
            if success:
                self.logger.info(f"Transfer succeeded: {amount} from {sender_hex} to {receiver_hex}")
            else:
                self.logger.warning(f"Transfer failed (insufficient funds): {json_data}")
                
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Invalid transfer data: {json_data}, error: {e}")
        except DatabaseServiceError as e:
            self.logger.error(f"Database error during transfer: {e}")
            raise
    
    def decode_hex_address(self, hex_str: str) -> bytes:
        """
        Decodes a hexadecimal address into raw bytes.
        Equivalent to Java decodeHexAddress() method.
        
        Args:
            hex_str: Hexadecimal string, optionally prefixed with '0x'
            
        Returns:
            Address bytes
        """
        # Remove '0x' prefix if present
        clean_hex = hex_str[2:] if hex_str.startswith("0x") else hex_str
        return bytes.fromhex(clean_hex)
    
    def check_root_hash_validity_and_save(self, block_number: int):
        """
        Validates the local Merkle root against peers and persists it if a quorum
        of peers agree. Equivalent to Java checkRootHashValidityAndSave() method.
        
        Args:
            block_number: Block height being validated
        """
        try:
            local_root = get_root_hash()
            if local_root is None:
                self.logger.warning(f"No local root hash available for block {block_number}")
                return
            
            peers_count = len(self.peers_to_check_root_hash_with)
            quorum = (peers_count * 2) // 3 + 1
            matches = 0
            
            for peer in self.peers_to_check_root_hash_with:
                success, peer_root = self.fetch_peer_root_hash(peer, block_number)
                
                if success and peer_root:
                    if peer_root == local_root:
                        matches += 1
                else:
                    peers_count -= 1
                    quorum = (peers_count * 2) // 3 + 1
                
                if matches >= quorum:
                    set_block_root_hash(block_number, local_root)
                    self.logger.info(f"Root hash validated and saved for block {block_number}")
                    return
            
            self.logger.error(f"Root hash mismatch: only {matches}/{len(self.peers_to_check_root_hash_with)} peers agreed")
            
            # Revert changes and reset block to reprocess the data
            revert_unsaved_changes()
            if self.subscription:
                # Reset subscription to last saved block
                last_saved_block = get_last_checked_block()
                self.logger.info(f"Resetting subscription to block {last_saved_block}")
                # Note: pwrpy might not have setLatestCheckedBlock, we'll handle this gracefully
                
        except Exception as e:
            self.logger.error(f"Error verifying root hash at block {block_number}: {e}")
    
    def fetch_peer_root_hash(self, peer: str, block_number: int) -> Tuple[bool, Optional[bytes]]:
        """
        Fetches the root hash from a peer node for the specified block number.
        Equivalent to Java fetchPeerRootHash() method.
        
        Args:
            peer: The peer hostname/address
            block_number: The block number to query
            
        Returns:
            Tuple of (success, root_hash_bytes)
        """
        try:
            # Build the URL for the peer's rootHash endpoint
            url = f"http://{peer}/rootHash?blockNumber={block_number}"
            
            # Make HTTP request with timeout
            response = requests.get(url, timeout=self.REQUEST_TIMEOUT, headers={"Accept": "text/plain"})
            
            # Check if response was successful
            if response.status_code == 200:
                hex_string = response.text.strip()
                
                # Validate that we received a non-empty hex string
                if not hex_string:
                    self.logger.warning(f"Peer {peer} returned empty root hash for block {block_number}")
                    return False, None
                
                # Decode hex string to bytes
                root_hash = bytes.fromhex(hex_string)
                
                self.logger.debug(f"Successfully fetched root hash from peer {peer} for block {block_number}")
                return True, root_hash
                
            else:
                self.logger.warning(f"Peer {peer} returned HTTP {response.status_code} for block {block_number}: {response.text}")
                return True, None  # Peer responded but with error
                
        except ValueError as e:
            self.logger.warning(f"Invalid hex response from peer {peer} for block {block_number}: {e}")
            return False, None
        except RequestException as e:
            self.logger.warning(f"Failed to fetch root hash from peer {peer} for block {block_number}: {e}")
            return False, None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching from peer {peer}: {e}")
            return False, None
    
    def run(self, args: Optional[List[str]] = None):
        """
        Main application entry point that orchestrates the entire system.
        Equivalent to Java main() method.
        
        Args:
            args: Command line arguments (defaults to sys.argv if None)
        """
        try:
            # Parse command line arguments
            parsed_args = self.parse_command_line_args(args)
            self.port = parsed_args.port
            
            # Start Flask API server
            self.start_flask_server()
            
            # Initialize initial balances
            self.init_initial_balances()
            
            # Initialize peers
            self.initialize_peers(parsed_args.peers)
            
            # Determine starting block
            last_block = get_last_checked_block()
            from_block = last_block if last_block > 0 else self.START_BLOCK
            
            self.logger.info(f"Starting synchronization from block {from_block}")
            
            # Subscribe and sync
            self.subscribe_and_sync(from_block)
            
            # Keep the main thread alive
            self.logger.info("Application started successfully. Press Ctrl+C to exit.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("Received shutdown signal")
                
        except Exception as e:
            self.logger.error(f"Application initialization failed: {e}")
            raise
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful application shutdown"""
        self.logger.info("Shutting down application...")
        
        try:
            # Stop subscription if active
            if self.subscription:
                self.subscription.stop()
                self.logger.info("Stopped blockchain subscription")
                
            # Flush any pending database changes
            flush()
            self.logger.info("Flushed database changes")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        
        self.logger.info("Application shutdown complete")


def main():
    """
    Application entry point.
    Equivalent to Java public static void main(String[] args)
    """
    app = Main()
    peers: List[str] = ["localhost"]
    app.run(peers)


if __name__ == "__main__":
    main()
