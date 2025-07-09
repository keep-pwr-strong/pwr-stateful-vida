import { MerkleTree } from "@pwrjs/core/services"

class DatabaseService {
    static #tree = null;
    static #initialized = false;
    static #LAST_CHECKED_BLOCK_KEY = Buffer.from('lastCheckedBlock');
    static #BLOCK_ROOT_PREFIX = 'blockRootHash_';

    /**
     * Initialize the DatabaseService. Must be called once before using any other methods.
     * This is equivalent to the static block in Rust.
     */
    static async initialize() {
        if (this.#initialized) {
            throw new Error('DatabaseService already initialized');
        }
        
        try {
            this.#tree = new MerkleTree('database');
            await this.#tree.ensureInitialized(); // Make sure the tree is ready
            this.#initialized = true;
            
            // Set up cleanup on process termination (equivalent to Rust shutdown hook)
            process.on('SIGINT', () => this.#cleanup());
            process.on('SIGTERM', () => this.#cleanup());
            process.on('exit', () => this.#cleanup());
            process.on('uncaughtException', () => this.#cleanup());
            process.on('unhandledRejection', () => this.#cleanup());
            
        } catch (error) {
            throw new Error(`Failed to initialize DatabaseService: ${error.message}`);
        }
    }

    /**
     * Get the global tree instance
     */
    static #getTree() {
        if (!this.#initialized || !this.#tree) {
            throw new Error('DatabaseService not initialized. Call initialize() first.');
        }
        return this.#tree;
    }

    /**
     * Cleanup resources
     */
    static async #cleanup() {
        if (this.#tree && !this.#tree.closed) {
            try {
                await this.#tree.close();
            } catch (error) {
                console.error('Error closing MerkleTree:', error);
            }
        }
    }

    /**
     * Get current Merkle root hash
     * 
     * @returns {Promise<Buffer|null>} Current Merkle root hash or null if tree is empty
     * @throws {Error} On RocksDB errors
     */
    static async getRootHash() {
        const tree = this.#getTree();
        return await tree.getRootHash();
    }

    /**
     * Flush pending writes to disk.
     * 
     * @throws {Error} On RocksDB errors
     */
    static async flush() {
        const tree = this.#getTree();
        return await tree.flushToDisk();
    }

    /**
     * Reverts all unsaved changes to the Merkle tree, restoring it to the last
     * flushed state. This is useful for rolling back invalid transactions or
     * when consensus validation fails.
     */
    static async revertUnsavedChanges() {
        const tree = this.#getTree();
        return await tree.revertUnsavedChanges();
    }

    /**
     * Retrieves the balance stored at the given address.
     * 
     * @param {Buffer|Uint8Array} address - 20-byte account address
     * @returns {Promise<bigint>} Non-negative balance, zero if absent
     * @throws {Error} On RocksDB errors or invalid address
     */
    static async getBalance(address) {
        if (!address || address.length === 0) {
            throw new Error('Address must not be empty');
        }

        const tree = this.#getTree();
        const data = await tree.getData(Buffer.from(address));
        
        if (!data || data.length === 0) {
            return 0n;
        }
        
        // Convert bytes to bigint (big-endian format, matching Rust's to_bytes_be)
        return this.#bytesToBigInt(data);
    }

    /**
     * Sets the balance for the given address.
     * 
     * @param {Buffer|Uint8Array} address - 20-byte account address
     * @param {bigint} balance - Non-negative balance
     * @throws {Error} On RocksDB errors or invalid parameters
     */
    static async setBalance(address, balance) {
        if (!address || address.length === 0) {
            throw new Error('Address must not be empty');
        }

        const tree = this.#getTree();
        const balanceBytes = this.#bigIntToBytes(balance);
        return await tree.addOrUpdateData(Buffer.from(address), balanceBytes);
    }

    /**
     * Transfers amount from sender to receiver.
     * 
     * @param {Buffer|Uint8Array} sender - Sender address
     * @param {Buffer|Uint8Array} receiver - Receiver address
     * @param {bigint} amount - Amount to transfer
     * @returns {Promise<boolean>} true if transfer succeeded, false on insufficient funds
     * @throws {Error} On RocksDB errors or invalid parameters
     */
    static async transfer(sender, receiver, amount) {
        if (!sender || sender.length === 0) {
            throw new Error('Sender address must not be empty');
        }
        if (!receiver || receiver.length === 0) {
            throw new Error('Receiver address must not be empty');
        }

        const senderBalance = await this.getBalance(sender);
        
        if (senderBalance < amount) {
            return false;
        }

        const newSenderBalance = senderBalance - amount;
        const receiverBalance = await this.getBalance(receiver);
        const newReceiverBalance = receiverBalance + amount;

        await this.setBalance(sender, newSenderBalance);
        await this.setBalance(receiver, newReceiverBalance);

        return true;
    }

    /**
     * Get the last checked block number, or zero if unset
     * 
     * @returns {Promise<number>} Last checked block number
     * @throws {Error} On RocksDB errors
     */
    static async getLastCheckedBlock() {
        const tree = this.#getTree();
        const data = await tree.getData(this.#LAST_CHECKED_BLOCK_KEY);
        
        if (!data || data.length < 8) {
            return 0;
        }

        // Read as big-endian u64 (matching Rust's to_be_bytes)
        const buffer = Buffer.from(data.slice(0, 8));
        return Number(buffer.readBigUInt64BE(0));
    }

    /**
     * Updates the last checked block number.
     * 
     * @param {number} blockNumber - Non-negative block number
     * @throws {Error} On RocksDB errors
     */
    static async setLastCheckedBlock(blockNumber) {
        const tree = this.#getTree();
        const buffer = Buffer.allocUnsafe(8);
        buffer.writeBigUInt64BE(BigInt(blockNumber), 0);
        return await tree.addOrUpdateData(this.#LAST_CHECKED_BLOCK_KEY, buffer);
    }

    /**
     * Records the Merkle root hash for a specific block.
     * 
     * @param {number} blockNumber - Block height
     * @param {Buffer|Uint8Array} rootHash - 32-byte Merkle root
     * @throws {Error} On RocksDB errors or invalid parameters
     */
    static async setBlockRootHash(blockNumber, rootHash) {
        if (!rootHash || rootHash.length === 0) {
            throw new Error('Root hash must not be empty');
        }

        const tree = this.#getTree();
        const key = Buffer.from(`${this.#BLOCK_ROOT_PREFIX}${blockNumber}`);
        return await tree.addOrUpdateData(key, Buffer.from(rootHash));
    }

    /**
     * Retrieves the Merkle root hash for a specific block.
     * 
     * @param {number} blockNumber - Block height
     * @returns {Promise<Buffer|null>} 32-byte root hash, or null if absent
     * @throws {Error} On RocksDB errors
     */
    static async getBlockRootHash(blockNumber) {
        const tree = this.#getTree();
        const key = Buffer.from(`${this.#BLOCK_ROOT_PREFIX}${blockNumber}`);
        return await tree.getData(key);
    }

    /**
     * Explicitly close the DatabaseService and underlying MerkleTree.
     * This should be called during application shutdown.
     * 
     * @throws {Error} On RocksDB errors
     */
    static async close() {
        if (this.#tree && !this.#tree.closed) {
            await this.#tree.close();
        }
        this.#initialized = false;
        this.#tree = null;
    }

    // Convenience functions for common operations

    /**
     * Check if an address has any balance
     * @param {Buffer|Uint8Array} address - Account address
     * @returns {Promise<boolean>} true if address has non-zero balance
     */
    static async hasBalance(address) {
        const balance = await this.getBalance(address);
        return balance > 0n;
    }

    /**
     * Add amount to an address (mint operation)
     * @param {Buffer|Uint8Array} address - Account address
     * @param {bigint} amount - Amount to mint
     */
    static async mint(address, amount) {
        const currentBalance = await this.getBalance(address);
        const newBalance = currentBalance + amount;
        return await this.setBalance(address, newBalance);
    }

    /**
     * Subtract amount from an address (burn operation)
     * @param {Buffer|Uint8Array} address - Account address
     * @param {bigint} amount - Amount to burn
     * @returns {Promise<boolean>} false if insufficient balance
     */
    static async burn(address, amount) {
        const currentBalance = await this.getBalance(address);
        
        if (currentBalance < amount) {
            return false;
        }

        const newBalance = currentBalance - amount;
        await this.setBalance(address, newBalance);
        return true;
    }

    /**
     * Get total number of accounts with balances
     * @returns {Promise<number>} Number of leaf nodes in the tree
     */
    static async getAccountCount() {
        const tree = this.#getTree();
        return await tree.getNumLeaves();
    }

    /**
     * Get tree depth
     * @returns {Promise<number>} Current depth of the Merkle tree
     */
    static async getTreeDepth() {
        const tree = this.#getTree();
        return await tree.getDepth();
    }

    // Private utility methods

    /**
     * Convert bigint to byte array (big-endian, matching Rust's to_bytes_be)
     * @param {bigint} value 
     * @returns {Buffer}
     */
    static #bigIntToBytes(value) {
        if (value === 0n) {
            return Buffer.from([0]);
        }
        
        const bytes = [];
        let num = value;
        while (num > 0n) {
            bytes.unshift(Number(num & 0xFFn));
            num = num >> 8n;
        }
        
        return Buffer.from(bytes);
    }

    /**
     * Convert byte array to bigint (big-endian, matching Rust's from_bytes_be)
     * @param {Buffer|Uint8Array} bytes 
     * @returns {bigint}
     */
    static #bytesToBigInt(bytes) {
        let result = 0n;
        for (let i = 0; i < bytes.length; i++) {
            result = (result << 8n) + BigInt(bytes[i]);
        }
        return result;
    }
}

export default DatabaseService;
