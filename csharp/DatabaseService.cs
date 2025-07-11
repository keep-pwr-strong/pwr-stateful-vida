using System;
using System.Numerics;
using System.Text;
using PWR.Utils.MerkleTree;

namespace PwrStatefulVIDA;

public static class DatabaseService
{
    private static MerkleTree? _tree = null;
    private static readonly object _lock = new object();
    private static readonly byte[] LAST_CHECKED_BLOCK_KEY = Encoding.UTF8.GetBytes("lastCheckedBlock");
    private const string BLOCK_ROOT_PREFIX = "blockRootHash_";

    public static void Initialize()
    {
        lock (_lock)
        {
            if (_tree != null)
            {
                throw new InvalidOperationException("DatabaseService already initialized");
            }

            _tree = new MerkleTree("database");
            
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

    private static MerkleTree GetTree()
    {
        if (_tree == null)
        {
            throw new InvalidOperationException("DatabaseService not initialized. Call Initialize() first.");
        }
        return _tree;
    }

    public static byte[]? GetRootHash()
    {
        var tree = GetTree();
        return tree.GetRootHash();
    }

    public static void Flush()
    {
        var tree = GetTree();
        tree.FlushToDisk();
    }

    public static void RevertUnsavedChanges()
    {
        var tree = GetTree();
        tree.RevertUnsavedChanges();
    }

    public static BigInteger GetBalance(byte[] address)
    {
        if (address == null || address.Length == 0)
        {
            throw new ArgumentException("Address must not be empty");
        }

        var tree = GetTree();
        var data = tree.GetData(address);

        if (data != null && data.Length > 0)
        {
            return new BigInteger(data, isUnsigned: true, isBigEndian: true);
        }

        return BigInteger.Zero;
    }

    public static void SetBalance(byte[] address, BigInteger balance)
    {
        if (address == null || address.Length == 0)
        {
            throw new ArgumentException("Address must not be empty");
        }

        var tree = GetTree();
        var balanceBytes = balance.ToByteArray(isUnsigned: true, isBigEndian: true);
        tree.AddOrUpdateData(address, balanceBytes);
    }

    public static bool Transfer(byte[] sender, byte[] receiver, BigInteger amount)
    {
        if (sender == null || sender.Length == 0)
        {
            throw new ArgumentException("Sender address must not be empty");
        }
        if (receiver == null || receiver.Length == 0)
        {
            throw new ArgumentException("Receiver address must not be empty");
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

    public static ulong GetLastCheckedBlock()
    {
        var tree = GetTree();
        var data = tree.GetData(LAST_CHECKED_BLOCK_KEY);

        if (data != null && data.Length >= 8)
        {
            try
            {
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(data, 0, 8);
                }
                return BitConverter.ToUInt64(data, 0);
            }
            catch (Exception)
            {
                throw new ArgumentException("Invalid block number format");
            }
        }

        return 0;
    }

    public static void SetLastCheckedBlock(ulong blockNumber)
    {
        var tree = GetTree();
        var blockBytes = BitConverter.GetBytes(blockNumber);
        
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(blockBytes);
        }
        
        tree.AddOrUpdateData(LAST_CHECKED_BLOCK_KEY, blockBytes);
    }

    public static void SetBlockRootHash(ulong blockNumber, byte[] rootHash)
    {
        if (rootHash == null || rootHash.Length == 0)
        {
            throw new ArgumentException("Root hash must not be empty");
        }

        var tree = GetTree();
        var key = Encoding.UTF8.GetBytes($"{BLOCK_ROOT_PREFIX}{blockNumber}");
        tree.AddOrUpdateData(key, rootHash);
    }

    public static byte[]? GetBlockRootHash(ulong blockNumber)
    {
        var tree = GetTree();
        var key = Encoding.UTF8.GetBytes($"{BLOCK_ROOT_PREFIX}{blockNumber}");
        return tree.GetData(key);
    }

    public static void Close()
    {
        lock (_lock)
        {
            if (_tree != null)
            {
                _tree.Close();
                _tree = null;
            }
        }
    }
}
