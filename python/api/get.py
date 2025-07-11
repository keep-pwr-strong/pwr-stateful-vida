"""
GET API: Python conversion of Java GET.java

Flask-based REST API endpoint handlers.
Currently provides the /rootHash endpoint for retrieving Merkle root hashes
for specific block numbers.
"""

from flask import Flask, request
import sys
import os
from typing import Optional

# Add the src directory to the path to import DatabaseService
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Import DatabaseService functions
from database_service import (
    get_root_hash,
    get_last_checked_block, 
    get_block_root_hash,
    DatabaseServiceError
)

# Flask app instance
app = Flask(__name__)

def bytes_to_hex_string(data: Optional[bytes]) -> Optional[str]:
    """Convert bytes to hex string"""
    if data is None:
        return None
    return data.hex()

@app.route('/rootHash', methods=['GET'])
def root_hash_endpoint():
    """GET /rootHash endpoint"""
    try:
        # Parse blockNumber query parameter
        block_number_str = request.args.get('blockNumber')
        if block_number_str is None:
            return "Missing blockNumber parameter", 400
        
        try:
            block_number = int(block_number_str)
        except ValueError:
            return "Invalid block number format", 400
        
        # Get last checked block for validation
        last_checked_block = get_last_checked_block()
        
        if block_number == last_checked_block:
            # Return current root hash
            root_hash = get_root_hash()
            if root_hash is not None:
                return bytes_to_hex_string(root_hash)
            else:
                return "Root hash not available", 400
                
        elif block_number < last_checked_block and block_number > 1:
            # Return historical root hash
            block_root_hash = get_block_root_hash(block_number)
            if block_root_hash is not None:
                return bytes_to_hex_string(block_root_hash)
            else:
                return f"Block root hash not found for block number: {block_number}", 400
        else:
            # Invalid block number
            return "Invalid block number", 400
            
    except DatabaseServiceError:
        return "Database error", 500
    except Exception:
        return "", 500

def run():
    """Initializes and registers all GET endpoint handlers with Flask"""
    # The route is already registered via the @app.route decorator
    pass

if __name__ == "__main__":
    app.run(debug=True)
