import sys
import threading
import time
from database_service import (
    get_last_checked_block, set_balance
)
from api.get import app as flask_app
from handler import subscribe_and_sync, peers_to_check_root_hash_with

START_BLOCK = 1
PORT = 8080

INITIAL_BALANCES = {
    bytes.fromhex("c767ea1d613eefe0ce1610b18cb047881bafb829"): 1_000_000_000_000,
    bytes.fromhex("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4"): 1_000_000_000_000,
    bytes.fromhex("9282d39ca205806473f4fde5bac48ca6dfb9d300"): 1_000_000_000_000,
    bytes.fromhex("e68191b7913e72e6f1759531fbfaa089ff02308a"): 1_000_000_000_000,
}

flask_thread = None

# Initializes peer list from arguments or defaults
def initialize_peers():
    if len(sys.argv) > 1:
        peers_to_check_root_hash_with.clear()
        peers_to_check_root_hash_with.extend(sys.argv[1:])
        print(f"Using peers from args: {peers_to_check_root_hash_with}")
    else:
        peers_to_check_root_hash_with.clear()
        peers_to_check_root_hash_with.extend([
            "localhost:8080"
        ])
        print(f"Using default peers: {peers_to_check_root_hash_with}")

# Sets up the initial account balances when starting from a fresh database
def init_initial_balances():
    if get_last_checked_block() == 0:
        print("Setting up initial balances for fresh database")
        
        for address, balance in INITIAL_BALANCES.items():
            set_balance(address, balance)
            print(f"Set initial balance for {address.hex()}: {balance}")
        print("Initial balances setup completed")

# Start the API server in a background task
def start_api_server():
    global flask_thread
    
    def run_flask():
        try:
            print(f"Starting Flask API server on port {PORT}")
            
            # Disable Flask request logging
            import logging
            logging.getLogger('werkzeug').setLevel(logging.ERROR)
            
            flask_app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)
        except Exception as e:
            print(f"Flask server error: {e}")
    
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    time.sleep(2)
    print(f"Flask API server started on http://0.0.0.0:{PORT}")

# Application entry point for synchronizing VIDA transactions
# with the local Merkle-backed database.
def main():
    print("Starting PWR VIDA Transaction Synchronizer...")
    
    initialize_peers()
    start_api_server()
    init_initial_balances()
    
    last_block = get_last_checked_block()
    from_block = last_block if last_block > 0 else START_BLOCK
    
    print(f"Starting synchronization from block {from_block}")
    
    subscribe_and_sync(from_block)

if __name__ == "__main__":
    main()
    