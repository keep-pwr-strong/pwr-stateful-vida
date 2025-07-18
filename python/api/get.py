from flask import Flask, request
from typing import Optional
from database_service import DatabaseService, DatabaseServiceError

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
        last_checked_block = DatabaseService.get_last_checked_block()
        
        if block_number == last_checked_block:
            # Return current root hash
            root_hash = DatabaseService.get_root_hash()
            if root_hash is not None:
                return bytes_to_hex_string(root_hash)
            else:
                return "Root hash not available", 400
                
        elif block_number < last_checked_block and block_number > 1:
            # Return historical root hash
            block_root_hash = DatabaseService.get_block_root_hash(block_number)
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
