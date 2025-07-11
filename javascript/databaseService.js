import { MerkleTree } from "@pwrjs/core/services"

class DatabaseService {
    static #tree = null;
    static #initialized = false;
    static #LAST_CHECKED_BLOCK_KEY = Buffer.from('lastCheckedBlock');
    static #BLOCK_ROOT_PREFIX = 'blockRootHash_';

    static async initialize() {
        if (this.#initialized) {
            throw new Error('DatabaseService already initialized');
        }
        
        try {
            this.#tree = new MerkleTree('database');
            await this.#tree.ensureInitialized();
            this.#initialized = true;
            
            process.on('SIGINT', () => this.#cleanup());
            process.on('SIGTERM', () => this.#cleanup());
            process.on('exit', () => this.#cleanup());
            process.on('uncaughtException', () => this.#cleanup());
            process.on('unhandledRejection', () => this.#cleanup());
            
        } catch (error) {
            throw new Error(`Failed to initialize DatabaseService: ${error.message}`);
        }
    }

    static #getTree() {
        if (!this.#initialized || !this.#tree) {
            throw new Error('DatabaseService not initialized. Call initialize() first.');
        }
        return this.#tree;
    }

    static async #cleanup() {
        if (this.#tree && !this.#tree.closed) {
            try {
                await this.#tree.close();
            } catch (error) {
                console.error('Error closing MerkleTree:', error);
            }
        }
    }

    static async getRootHash() {
        const tree = this.#getTree();
        return await tree.getRootHash();
    }

    static async flush() {
        const tree = this.#getTree();
        return await tree.flushToDisk();
    }

    static async revertUnsavedChanges() {
        const tree = this.#getTree();
        return await tree.revertUnsavedChanges();
    }

    static async getBalance(address) {
        if (!address || address.length === 0) {
            throw new Error('Address must not be empty');
        }

        const tree = this.#getTree();
        const data = await tree.getData(Buffer.from(address));
        
        if (!data || data.length === 0) {
            return 0n;
        }
        
        return this.#bytesToBigInt(data);
    }

    static async setBalance(address, balance) {
        if (!address || address.length === 0) {
            throw new Error('Address must not be empty');
        }

        const tree = this.#getTree();
        const balanceBytes = this.#bigIntToBytes(balance);
        return await tree.addOrUpdateData(Buffer.from(address), balanceBytes);
    }

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

    static async getLastCheckedBlock() {
        const tree = this.#getTree();
        const data = await tree.getData(this.#LAST_CHECKED_BLOCK_KEY);
        
        if (!data || data.length < 8) {
            return 0;
        }

        const buffer = Buffer.from(data.slice(0, 8));
        return Number(buffer.readBigUInt64BE(0));
    }

    static async setLastCheckedBlock(blockNumber) {
        const tree = this.#getTree();
        const buffer = Buffer.allocUnsafe(8);
        buffer.writeBigUInt64BE(BigInt(blockNumber), 0);
        return await tree.addOrUpdateData(this.#LAST_CHECKED_BLOCK_KEY, buffer);
    }

    static async setBlockRootHash(blockNumber, rootHash) {
        if (!rootHash || rootHash.length === 0) {
            throw new Error('Root hash must not be empty');
        }

        const tree = this.#getTree();
        const key = Buffer.from(`${this.#BLOCK_ROOT_PREFIX}${blockNumber}`);
        return await tree.addOrUpdateData(key, Buffer.from(rootHash));
    }

    static async getBlockRootHash(blockNumber) {
        const tree = this.#getTree();
        const key = Buffer.from(`${this.#BLOCK_ROOT_PREFIX}${blockNumber}`);
        return await tree.getData(key);
    }

    static async close() {
        if (this.#tree && !this.#tree.closed) {
            await this.#tree.close();
        }
        this.#initialized = false;
        this.#tree = null;
    }

    static #bytesToBigInt(bytes) {
        if (bytes.length === 0) return 0n;
        let result = 0n;
        for (let i = 0; i < bytes.length; i++) {
            result = (result << 8n) | BigInt(bytes[i]);
        }
        return result;
    }

    static #bigIntToBytes(bigint) {
        if (bigint === 0n) return Buffer.from([0]);
        
        const bytes = [];
        let value = bigint;
        while (value > 0n) {
            bytes.unshift(Number(value & 0xFFn));
            value = value >> 8n;
        }
        return Buffer.from(bytes);
    }
}

export default DatabaseService;
