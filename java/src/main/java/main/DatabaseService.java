package main;

import io.pwrlabs.database.rocksdb.MerkleTree;
import org.rocksdb.RocksDBException;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Singleton service for interacting with the underlying RocksDB-backed MerkleTree.
 * Provides methods for managing account balances, transfers, block tracking, and
 * Merkle root hash operations. All operations may throw RocksDBException.
 *
 * <p>This service maintains:
 * <ul>
 *   <li>Account balances stored in the Merkle tree</li>
 *   <li>Last checked block number for synchronization</li>
 *   <li>Historical block root hashes for validation</li>
 * </ul>
 *
 * <p>The underlying MerkleTree is automatically closed on JVM shutdown.
 */
public final class DatabaseService {
    private static final MerkleTree TREE;
    private static final byte[] LAST_CHECKED_BLOCK_KEY = "lastCheckedBlock".getBytes(StandardCharsets.UTF_8);
    private static final String BLOCK_ROOT_PREFIX = "blockRootHash_";

    static {
        try {
            TREE = new MerkleTree("database");
        } catch (RocksDBException e) {
            throw new ExceptionInInitializerError("Failed to initialize MerkleTree: " + e.getMessage());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                TREE.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    /**
     * @return current Merkle root hash
     * @throws RocksDBException on RocksDB errors
     */
    public static byte[] getRootHash() throws RocksDBException {
        return TREE.getRootHash();
    }

    /**
     * Flush pending writes to disk.
     * @throws RocksDBException on RocksDB errors
     */
    public static void flush() throws RocksDBException {
        TREE.flushToDisk();
    }

    /**
     * Reverts all unsaved changes to the Merkle tree, restoring it to the last
     * flushed state. This is useful for rolling back invalid transactions or
     * when consensus validation fails.
     */
    public static void revertUnsavedChanges() {
            TREE.revertUnsavedChanges();
    }

    /**
     * Retrieves the balance stored at the given address.
     *
     * @param address 20-byte account address
     * @return non-negative balance, zero if absent
     * @throws RocksDBException on RocksDB errors
     */
    public static BigInteger getBalance(byte[] address) throws RocksDBException {
        Objects.requireNonNull(address, "Address must not be null");
        byte[] data = TREE.getData(address);
        if (data == null || data.length == 0) {
            return BigInteger.ZERO;
        }
        return new BigInteger(1, data);
    }

    /**
     * Sets the balance for the given address.
     *
     * @param address 20-byte account address
     * @param balance non-negative balance
     * @throws RocksDBException on RocksDB errors
     */
    public static void setBalance(byte[] address, BigInteger balance) throws RocksDBException {
        Objects.requireNonNull(address, "Address must not be null");
        Objects.requireNonNull(balance, "Balance must not be null");
        TREE.addOrUpdateData(address, balance.toByteArray());
    }

    /**
     * Transfers amount from sender to receiver.
     *
     * @param sender   sender address
     * @param receiver receiver address
     * @param amount   amount to transfer
     * @return true if transfer succeeded, false on insufficient funds
     * @throws RocksDBException on RocksDB errors
     */
    public static boolean transfer(byte[] sender, byte[] receiver, BigInteger amount) throws RocksDBException {
        Objects.requireNonNull(sender);
        Objects.requireNonNull(receiver);
        Objects.requireNonNull(amount);
        BigInteger senderBal = getBalance(sender);
        if (senderBal.compareTo(amount) < 0) {
            return false;
        }
        setBalance(sender, senderBal.subtract(amount));
        setBalance(receiver, getBalance(receiver).add(amount));
        return true;
    }

    /**
     * @return the last checked block number, or zero if unset
     * @throws RocksDBException on RocksDB errors
     */
    public static long getLastCheckedBlock() throws RocksDBException {
        byte[] bytes = TREE.getData(LAST_CHECKED_BLOCK_KEY);
        return (bytes == null || bytes.length < Long.BYTES)
                ? 0L
                : ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * Updates the last checked block number.
     *
     * @param blockNumber non-negative block number
     * @throws RocksDBException on RocksDB errors
     */
    public static void setLastCheckedBlock(long blockNumber) throws RocksDBException {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).putLong(blockNumber);
        TREE.addOrUpdateData(LAST_CHECKED_BLOCK_KEY, buf.array());
    }

    /**
     * Records the Merkle root hash for a specific block.
     *
     * @param blockNumber block height
     * @param rootHash    32-byte Merkle root
     * @throws RocksDBException on RocksDB errors
     */
    public static void setBlockRootHash(long blockNumber, byte[] rootHash) throws RocksDBException {
        Objects.requireNonNull(rootHash, "Root hash must not be null");
        String key = BLOCK_ROOT_PREFIX + blockNumber;
        TREE.addOrUpdateData(key.getBytes(StandardCharsets.UTF_8), rootHash);
    }

    /**
     * Retrieves the Merkle root hash for a specific block.
     *
     * @param blockNumber block height
     * @return 32-byte root hash, or null if absent
     * @throws RocksDBException on RocksDB errors
     */
    public static byte[] getBlockRootHash(long blockNumber) throws RocksDBException {
        String key = BLOCK_ROOT_PREFIX + blockNumber;
        return TREE.getData(key.getBytes(StandardCharsets.UTF_8));
    }
}
