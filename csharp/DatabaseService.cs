using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;
using PWR.Utils.MerkleTree;

namespace PwrStatefulVIDA;

/// <summary>
/// Singleton service for interacting with the underlying LiteDB-backed MerkleTree.
/// Provides methods for managing account balances, transfers, block tracking, and
/// Merkle root hash operations. All operations may throw exceptions.
/// 
/// This service maintains:
/// - Account balances stored in the Merkle tree
/// - Last checked block number for synchronization
/// - Historical block root hashes for validation
/// 
/// The underlying MerkleTree is automatically closed on program shutdown.
/// </summary>
public static class DatabaseService
{
    // Global static instance of the MerkleTree
    private static MerkleTree? _tree = null;
    private static readonly object _lock = new object();

    // Constants
    private static readonly byte[] LAST_CHECKED_BLOCK_KEY = Encoding.UTF8.GetBytes("lastCheckedBlock");
    private const string BLOCK_ROOT_PREFIX = "blockRootHash_";

    /// <summary>
    /// Initialize the DatabaseService. Must be called once before using any other methods.
    /// This is equivalent to the static block in Rust.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when DatabaseService is already initialized</exception>
    /// <exception cref="MerkleTreeException">Thrown on database initialization errors</exception>
    public static void Initialize()
    {
        lock (_lock)
        {
            if (_tree != null)
            {
                throw new IllegalStateException("DatabaseService already initialized");
            }

            _tree = new MerkleTree("database");
            
            // Set up shutdown hook equivalent
            AppDomain.CurrentDomain.ProcessExit += (sender, e) => 
            {
                try
                {
                    Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error during shutdown: {ex.Message}");
                }
            };
        }
    }

    /// <summary>
    /// Get the global tree instance
    /// </summary>
    /// <returns>The MerkleTree instance</returns>
    /// <exception cref="IllegalStateException">Thrown when DatabaseService is not initialized</exception>
    private static MerkleTree GetTree()
    {
        if (_tree == null)
        {
            throw new IllegalStateException("DatabaseService not initialized. Call Initialize() first.");
        }
        return _tree;
    }

    /// <summary>
    /// Get current Merkle root hash
    /// </summary>
    /// <returns>Current Merkle root hash or null if tree is empty</returns>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static byte[]? GetRootHash()
    {
        var tree = GetTree();
        return tree.GetRootHash();
    }

    /// <summary>
    /// Flush pending writes to disk.
    /// </summary>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static void Flush()
    {
        var tree = GetTree();
        tree.FlushToDisk();
    }

    /// <summary>
    /// Reverts all unsaved changes to the Merkle tree, restoring it to the last
    /// flushed state. This is useful for rolling back invalid transactions or
    /// when consensus validation fails.
    /// </summary>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static void RevertUnsavedChanges()
    {
        var tree = GetTree();
        tree.RevertUnsavedChanges();
    }

    /// <summary>
    /// Retrieves the balance stored at the given address.
    /// </summary>
    /// <param name="address">20-byte account address</param>
    /// <returns>Non-negative balance, zero if absent</returns>
    /// <exception cref="InvalidArgumentException">Thrown when address is empty</exception>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static BigInteger GetBalance(byte[] address)
    {
        if (address == null || address.Length == 0)
        {
            throw new InvalidArgumentException("Address must not be empty");
        }

        var tree = GetTree();
        var data = tree.GetData(address);

        if (data != null && data.Length > 0)
        {
            return new BigInteger(data, isUnsigned: true, isBigEndian: true);
        }

        return BigInteger.Zero;
    }

    /// <summary>
    /// Sets the balance for the given address.
    /// </summary>
    /// <param name="address">20-byte account address</param>
    /// <param name="balance">Non-negative balance</param>
    /// <exception cref="InvalidArgumentException">Thrown when address is empty</exception>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static void SetBalance(byte[] address, BigInteger balance)
    {
        if (address == null || address.Length == 0)
        {
            throw new InvalidArgumentException("Address must not be empty");
        }

        var tree = GetTree();
        var balanceBytes = balance.ToByteArray(isUnsigned: true, isBigEndian: true);
        tree.AddOrUpdateData(address, balanceBytes);
    }

    /// <summary>
    /// Transfers amount from sender to receiver.
    /// </summary>
    /// <param name="sender">Sender address</param>
    /// <param name="receiver">Receiver address</param>
    /// <param name="amount">Amount to transfer</param>
    /// <returns>true if transfer succeeded, false on insufficient funds</returns>
    /// <exception cref="InvalidArgumentException">Thrown when addresses are empty</exception>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static bool Transfer(byte[] sender, byte[] receiver, BigInteger amount)
    {
        if (sender == null || sender.Length == 0)
        {
            throw new InvalidArgumentException("Sender address must not be empty");
        }
        if (receiver == null || receiver.Length == 0)
        {
            throw new InvalidArgumentException("Receiver address must not be empty");
        }

        var senderBalance = GetBalance(sender);

        if (senderBalance < amount)
        {
            return false;
        }

        var newSenderBalance = senderBalance - amount;
        var receiverBalance = GetBalance(receiver);
        var newReceiverBalance = receiverBalance + amount;

        SetBalance(sender, newSenderBalance);
        SetBalance(receiver, newReceiverBalance);

        return true;
    }

    /// <summary>
    /// Get the last checked block number, or zero if unset
    /// </summary>
    /// <returns>Last checked block number</returns>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static ulong GetLastCheckedBlock()
    {
        var tree = GetTree();
        var data = tree.GetData(LAST_CHECKED_BLOCK_KEY);

        if (data != null && data.Length >= 8)
        {
            try
            {
                // Convert big-endian bytes to ulong
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(data, 0, 8);
                }
                return BitConverter.ToUInt64(data, 0);
            }
            catch (Exception)
            {
                throw new InvalidArgumentException("Invalid block number format");
            }
        }

        return 0;
    }

    /// <summary>
    /// Updates the last checked block number.
    /// </summary>
    /// <param name="blockNumber">Non-negative block number</param>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static void SetLastCheckedBlock(ulong blockNumber)
    {
        var tree = GetTree();
        var blockBytes = BitConverter.GetBytes(blockNumber);
        
        // Convert to big-endian if necessary
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(blockBytes);
        }
        
        tree.AddOrUpdateData(LAST_CHECKED_BLOCK_KEY, blockBytes);
    }

    /// <summary>
    /// Records the Merkle root hash for a specific block.
    /// </summary>
    /// <param name="blockNumber">Block height</param>
    /// <param name="rootHash">32-byte Merkle root</param>
    /// <exception cref="InvalidArgumentException">Thrown when root hash is empty</exception>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static void SetBlockRootHash(ulong blockNumber, byte[] rootHash)
    {
        if (rootHash == null || rootHash.Length == 0)
        {
            throw new InvalidArgumentException("Root hash must not be empty");
        }

        var tree = GetTree();
        var key = Encoding.UTF8.GetBytes($"{BLOCK_ROOT_PREFIX}{blockNumber}");
        tree.AddOrUpdateData(key, rootHash);
    }

    /// <summary>
    /// Retrieves the Merkle root hash for a specific block.
    /// </summary>
    /// <param name="blockNumber">Block height</param>
    /// <returns>32-byte root hash, or null if absent</returns>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static byte[]? GetBlockRootHash(ulong blockNumber)
    {
        var tree = GetTree();
        var key = Encoding.UTF8.GetBytes($"{BLOCK_ROOT_PREFIX}{blockNumber}");
        return tree.GetData(key);
    }

    /// <summary>
    /// Explicitly close the DatabaseService and underlying MerkleTree.
    /// This should be called during application shutdown.
    /// </summary>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static void Close()
    {
        lock (_lock)
        {
            _tree?.Close();
            _tree = null;
        }
    }

    // Convenience functions for common operations

    /// <summary>
    /// Check if an address has any balance
    /// </summary>
    /// <param name="address">Account address</param>
    /// <returns>true if address has non-zero balance</returns>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static bool HasBalance(byte[] address)
    {
        var balance = GetBalance(address);
        return balance > BigInteger.Zero;
    }

    /// <summary>
    /// Add amount to an address (mint operation)
    /// </summary>
    /// <param name="address">Account address</param>
    /// <param name="amount">Amount to mint</param>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static void Mint(byte[] address, BigInteger amount)
    {
        var currentBalance = GetBalance(address);
        var newBalance = currentBalance + amount;
        SetBalance(address, newBalance);
    }

    /// <summary>
    /// Subtract amount from an address (burn operation)
    /// </summary>
    /// <param name="address">Account address</param>
    /// <param name="amount">Amount to burn</param>
    /// <returns>false if insufficient balance</returns>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static bool Burn(byte[] address, BigInteger amount)
    {
        var currentBalance = GetBalance(address);

        if (currentBalance < amount)
        {
            return false;
        }

        var newBalance = currentBalance - amount;
        SetBalance(address, newBalance);
        return true;
    }

    /// <summary>
    /// Get total number of accounts with balances
    /// </summary>
    /// <returns>Number of leaf nodes in the tree</returns>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static int GetAccountCount()
    {
        var tree = GetTree();
        return tree.GetNumLeaves();
    }

    /// <summary>
    /// Get tree depth
    /// </summary>
    /// <returns>Current depth of the Merkle tree</returns>
    /// <exception cref="MerkleTreeException">Thrown on database errors</exception>
    public static int GetTreeDepth()
    {
        var tree = GetTree();
        return tree.GetDepth();
    }
}

// Test class for DatabaseService
public static class DatabaseServiceTest
{
    public static void TestDatabaseService()
    {
        Console.WriteLine("=== C# DatabaseService Test ===");

        try
        {
            // Initialize the service
            DatabaseService.Initialize();
            Console.WriteLine("✓ DatabaseService initialized");

            // Test initial state
            var rootHash = DatabaseService.GetRootHash();
            Console.WriteLine($"Initial root hash: {(rootHash != null ? Convert.ToHexString(rootHash).ToLowerInvariant() : "null")}");
            Console.WriteLine($"Initial account count: {DatabaseService.GetAccountCount()}");
            Console.WriteLine($"Initial tree depth: {DatabaseService.GetTreeDepth()}");

            // Create test addresses
            var address1 = Convert.FromHexString("1234567890123456789012345678901234567890");
            var address2 = Convert.FromHexString("abcdefabcdefabcdefabcdefabcdefabcdefabcd");

            // Test minting
            DatabaseService.Mint(address1, new BigInteger(1000));
            DatabaseService.Mint(address2, new BigInteger(500));
            Console.WriteLine($"✓ Minted balances - Address1: {DatabaseService.GetBalance(address1)}, Address2: {DatabaseService.GetBalance(address2)}");

            // Test transfer
            var transferResult = DatabaseService.Transfer(address1, address2, new BigInteger(300));
            Console.WriteLine($"✓ Transfer result: {transferResult}");
            Console.WriteLine($"After transfer - Address1: {DatabaseService.GetBalance(address1)}, Address2: {DatabaseService.GetBalance(address2)}");

            // Test block operations
            DatabaseService.SetLastCheckedBlock(12345);
            Console.WriteLine($"✓ Last checked block: {DatabaseService.GetLastCheckedBlock()}");

            // Test block root hash
            var testRootHash = Convert.FromHexString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
            DatabaseService.SetBlockRootHash(12345, testRootHash);
            var retrievedHash = DatabaseService.GetBlockRootHash(12345);
            Console.WriteLine($"✓ Block root hash stored and retrieved: {(retrievedHash != null ? Convert.ToHexString(retrievedHash).ToLowerInvariant() : "null")}");

            // Test burn
            var burnResult = DatabaseService.Burn(address1, new BigInteger(100));
            Console.WriteLine($"✓ Burn result: {burnResult}, New balance: {DatabaseService.GetBalance(address1)}");

            // Test final state
            var finalRootHash = DatabaseService.GetRootHash();
            Console.WriteLine($"Final root hash: {(finalRootHash != null ? Convert.ToHexString(finalRootHash).ToLowerInvariant() : "null")}");
            Console.WriteLine($"Final account count: {DatabaseService.GetAccountCount()}");
            Console.WriteLine($"Final tree depth: {DatabaseService.GetTreeDepth()}");

            // Flush to disk
            DatabaseService.Flush();
            Console.WriteLine("✓ Changes flushed to disk");

            // Close the service
            DatabaseService.Close();
            Console.WriteLine("✓ DatabaseService closed");

        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Test failed: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }

        Console.WriteLine("=== Test completed ===");
    }
}
