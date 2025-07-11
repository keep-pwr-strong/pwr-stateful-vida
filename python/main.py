import sys
import json
import threading
import time
import requests
from pwrpy.pwrsdk import PWRPY
from pwrpy.models.Transaction import VidaDataTransaction
from database_service import (
    get_root_hash, get_last_checked_block, set_last_checked_block, 
    set_balance, transfer, set_block_root_hash, revert_unsaved_changes, flush
)
from api.get import app as flask_app

VIDA_ID = 73_746_238
START_BLOCK = 1
RPC_URL = "https://pwrrpc.pwrlabs.io/"
PORT = 8080
REQUEST_TIMEOUT = 10

INITIAL_BALANCES = {
    bytes.fromhex("c767ea1d613eefe0ce1610b18cb047881bafb829"): 1_000_000_000_000,
    bytes.fromhex("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4"): 1_000_000_000_000,
    bytes.fromhex("9282d39ca205806473f4fde5bac48ca6dfb9d300"): 1_000_000_000_000,
    bytes.fromhex("e68191b7913e72e6f1759531fbfaa089ff02308a"): 1_000_000_000_000,
}

pwrpy_client = None
peers_to_check_root_hash_with = []
subscription = None
flask_thread = None

def main():
    print("Starting PWR VIDA Transaction Synchronizer...")
    
    initialize_peers()
    start_flask_server()
    init_initial_balances()
    
    last_block = get_last_checked_block()
    from_block = last_block if last_block > 0 else START_BLOCK
    
    print(f"Starting synchronization from block {from_block}")
    
    subscribe_and_sync(from_block)


def start_flask_server():
    global flask_thread
    
    def run_flask():
        try:
            print(f"Starting Flask API server on port {PORT}")
            flask_app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)
        except Exception as e:
            print(f"Flask server error: {e}")
    
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    time.sleep(2)
    print(f"Flask API server started on http://0.0.0.0:{PORT}")

def init_initial_balances():
    if get_last_checked_block() == 0:
        print("Setting up initial balances for fresh database")
        
        for address, balance in INITIAL_BALANCES.items():
            set_balance(address, balance)
            print(f"Set initial balance for {address.hex()}: {balance}")
        
        flush()
        print("Initial balances setup completed")

def initialize_peers():
    global peers_to_check_root_hash_with
    
    if len(sys.argv) > 1:
        peers_to_check_root_hash_with = sys.argv[1:]
        print(f"Using peers from args: {peers_to_check_root_hash_with}")
    else:
        peers_to_check_root_hash_with = [
            "localhost:8080"
        ]
        print(f"Using default peers: {peers_to_check_root_hash_with}")

def subscribe_and_sync(from_block):
    global pwrpy_client, subscription
    
    print(f"Starting VIDA transaction subscription from block {from_block}")
    
    pwrpy_client = PWRPY(RPC_URL)
    
    subscription = pwrpy_client.subscribe_to_vida_transactions(
        VIDA_ID,
        from_block,
        process_transaction
    )
    
    print(f"Successfully subscribed to VIDA {VIDA_ID} transactions")
    
    start_block_progress_monitor()

def start_block_progress_monitor():
    def monitor_blocks():
        last_checked = get_last_checked_block()
        
        while True:
            try:
                if subscription is None:
                    break
                
                current_block = subscription.get_latest_checked_block()
                
                if current_block > last_checked:
                    on_chain_progress(current_block)
                    last_checked = current_block
                
                time.sleep(5)
                
            except Exception as e:
                print(f"Error in block progress monitor: {e}")
                time.sleep(10)
    
    monitor_thread = threading.Thread(target=monitor_blocks, daemon=True)
    monitor_thread.start()
    print("Block progress monitor started")

def on_chain_progress(block_number):
    set_last_checked_block(block_number)
    check_root_hash_validity_and_save(block_number)
    print(f"Checkpoint updated to block {block_number}")

def process_transaction(txn):
    try:
        print(f"TRANSACTION RECEIVED: {txn.data}")
        data_hex = txn.data
        data_bytes = bytes.fromhex(data_hex)
        
        data_str = data_bytes.decode('utf-8')
        json_data = json.loads(data_str)
        
        action = json_data.get('action', '')
        
        if action.lower() == 'transfer':
            handle_transfer(json_data, txn.sender)
            
    except Exception as e:
        print(f"Error processing transaction: {e}")

def handle_transfer(json_data, sender_hex):
    try:
        amount = int(json_data.get('amount', 0))
        receiver_hex = json_data.get('receiver', '')
        
        if amount <= 0 or not receiver_hex:
            print(f"Invalid transfer data: {json_data}")
            return
        
        sender = decode_hex_address(sender_hex)
        receiver = decode_hex_address(receiver_hex)
        
        success = transfer(sender, receiver, amount)
        
        if success:
            print(f"Transfer succeeded: {amount} from {sender_hex} to {receiver_hex}")
        else:
            print(f"Transfer failed (insufficient funds): {amount} from {sender_hex} to {receiver_hex}")
            
    except Exception as e:
        print(f"Error handling transfer: {e}")

def decode_hex_address(hex_str):
    clean_hex = hex_str[2:] if hex_str.startswith('0x') else hex_str
    return bytes.fromhex(clean_hex)

def check_root_hash_validity_and_save(block_number):
    local_root = get_root_hash()
    
    if not local_root:
        print(f"No local root hash available for block {block_number}")
        return
    
    peers_count = len(peers_to_check_root_hash_with)
    quorum = (peers_count * 2) // 3 + 1
    matches = 0
    
    for peer in peers_to_check_root_hash_with:
        success, peer_root = fetch_peer_root_hash(peer, block_number)
        
        if success and peer_root:
            if peer_root == local_root:
                matches += 1
        else:
            if peers_count > 0:
                peers_count -= 1
                quorum = (peers_count * 2) // 3 + 1
        
        if matches >= quorum:
            set_block_root_hash(block_number, local_root)
            print(f"Root hash validated and saved for block {block_number}")
            return
    
    print(f"Root hash mismatch: only {matches}/{len(peers_to_check_root_hash_with)} peers agreed")
    revert_unsaved_changes()

def fetch_peer_root_hash(peer, block_number):
    url = f"http://{peer}/rootHash?blockNumber={block_number}"
    
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers={'Accept': 'text/plain'})
        
        if response.status_code == 200:
            hex_string = response.text.strip()
            
            if not hex_string:
                print(f"Peer {peer} returned empty root hash for block {block_number}")
                return False, None
            
            try:
                root_hash = bytes.fromhex(hex_string)
                print(f"Successfully fetched root hash from peer {peer} for block {block_number}")
                return True, root_hash
            except ValueError:
                print(f"Invalid hex response from peer {peer} for block {block_number}")
                return False, None
        else:
            print(f"Peer {peer} returned HTTP {response.status_code} for block {block_number}")
            return True, None
            
    except Exception:
        print(f"Failed to fetch root hash from peer {peer} for block {block_number}")
        return False, None

if __name__ == "__main__":
    main()
