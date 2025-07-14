import json
import threading
import time
import requests
from pwrpy.pwrsdk import PWRPY
from database_service import (
    get_root_hash, get_last_checked_block, set_last_checked_block,
    transfer, set_block_root_hash, revert_unsaved_changes
)

VIDA_ID = 73_746_238
RPC_URL = "https://pwrrpc.pwrlabs.io/"
REQUEST_TIMEOUT = 10

pwrpy_client = None
subscription = None

# Fetches the root hash from a peer node for the specified block number
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

# Validates the local Merkle root against peers and persists it if a quorum of peers agree
def check_root_hash_validity_and_save(block_number, peers):
    local_root = get_root_hash()
    
    if not local_root:
        print(f"No local root hash available for block {block_number}")
        return
    
    peers_count = len(peers)
    quorum = (peers_count * 2) // 3 + 1
    matches = 0
    
    for peer in peers:
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
    
    print(f"Root hash mismatch: only {matches}/{len(peers)} peers agreed")
    revert_unsaved_changes()

# Executes a token transfer described by the given JSON payload
def handle_transfer(json_data, sender_hex):
    try:
        amount = int(json_data.get('amount', 0))
        receiver_hex = json_data.get('receiver', '')
        
        if amount <= 0 or not receiver_hex:
            print(f"Invalid transfer data: {json_data}")
            return

        sender_address = sender_hex[2:] if sender_hex.startswith('0x') else sender_hex
        receiver_address = receiver_hex[2:] if receiver_hex.startswith('0x') else receiver_hex

        sender = bytes.fromhex(sender_address)
        receiver = bytes.fromhex(receiver_address)
        
        success = transfer(sender, receiver, amount)
        
        if success:
            print(f"Transfer succeeded: {amount} from {sender_hex} to {receiver_hex}")
        else:
            print(f"Transfer failed (insufficient funds): {amount} from {sender_hex} to {receiver_hex}")
            
    except Exception as e:
        print(f"Error handling transfer: {e}")

# Processes a single VIDA transaction
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

# Callback invoked as blocks are processed
def on_chain_progress(block_number, peers):
    set_last_checked_block(block_number)
    check_root_hash_validity_and_save(block_number, peers)
    print(f"Checkpoint updated to block {block_number}")

# Subscribes to VIDA transactions starting from the given block
def subscribe_and_sync(from_block, peers):
    global pwrpy_client, subscription
    
    print(f"Starting VIDA transaction subscription from block {from_block}")
    
    pwrpy_client = PWRPY(RPC_URL)
    
    subscription = pwrpy_client.subscribe_to_vida_transactions(
        VIDA_ID,
        from_block,
        process_transaction
    )
    
    print(f"Successfully subscribed to VIDA {VIDA_ID} transactions")
    
    def monitor_blocks():
        last_checked = get_last_checked_block()
        while True:
            try:
                if subscription is None:
                    break
                
                current_block = subscription.get_latest_checked_block()
                
                if current_block > last_checked:
                    on_chain_progress(current_block, peers)
                    last_checked = current_block
                
                time.sleep(5)
            except Exception as e:
                print(f"Error in block progress monitor: {e}")
                time.sleep(10)
    monitor_thread = threading.Thread(target=monitor_blocks, daemon=True)
    monitor_thread.start()
    print("Block progress monitor started")
