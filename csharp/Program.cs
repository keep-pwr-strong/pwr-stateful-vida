using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Newtonsoft.Json.Linq;
using PWR;
using PWR.Models;
using PWR.Utils;

namespace PwrStatefulVIDA;

public class Program
{
    // Constants
    private const ulong VIDA_ID = 73746238;
    private const ulong START_BLOCK = 1;
    private const string RPC_URL = "https://pwrrpc.pwrlabs.io/";
    private const int PORT = 8080;
    
    // Initial balances for fresh database
    private static readonly Dictionary<byte[], BigInteger> INITIAL_BALANCES = new()
    {
        { Convert.FromHexString("c767ea1d613eefe0ce1610b18cb047881bafb829"), new BigInteger(1000000000000) },
        { Convert.FromHexString("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4"), new BigInteger(1000000000000) },
        { Convert.FromHexString("9282d39ca205806473f4fde5bac48ca6dfb9d300"), new BigInteger(1000000000000) },
        { Convert.FromHexString("e68191b7913e72e6f1759531fbfaa089ff02308a"), new BigInteger(1000000000000) },
    };

    // Global state
    private static List<string> peersToCheckRootHashWith = new();
    private static RPC? pwrClient;
    private static VidaTransactionSubscription? subscription;
    private static Timer? blockProgressMonitor;

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting PWR VIDA Transaction Synchronizer...");
        
        InitializePeers(args);
        DatabaseService.Initialize();
        await StartApiServer();
        InitInitialBalances();
        
        var lastBlock = DatabaseService.GetLastCheckedBlock();
        var fromBlock = lastBlock > 0 ? lastBlock : START_BLOCK;
        
        Console.WriteLine($"Starting synchronization from block {fromBlock}");
        
        await SubscribeAndSync(fromBlock);
        
        Console.WriteLine("Application started successfully. Press Ctrl+C to exit.");
        
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            Environment.Exit(0);
        };
        
        await Task.Delay(Timeout.Infinite);
    }

    private static void InitializePeers(string[] args)
    {
        if (args.Length > 0)
        {
            peersToCheckRootHashWith = args.ToList();
            Console.WriteLine($"Using peers from args: [{string.Join(", ", peersToCheckRootHashWith)}]");
        }
        else
        {
            peersToCheckRootHashWith = new List<string> { "localhost:8080" };
            Console.WriteLine($"Using default peers: [{string.Join(", ", peersToCheckRootHashWith)}]");
        }
    }

    private static async Task StartApiServer()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{PORT}");
        
        var app = builder.Build();
        GET.Run(app);

        _ = Task.Run(async () =>
        {
            Console.WriteLine($"Starting API server on port {PORT}");
            await app.RunAsync();
        });

        await Task.Delay(2000);
        Console.WriteLine($"API server started on http://0.0.0.0:{PORT}");
    }

    private static void InitInitialBalances()
    {
        var lastCheckedBlock = DatabaseService.GetLastCheckedBlock();

        if (lastCheckedBlock == 0)
        {
            Console.WriteLine("Setting up initial balances for fresh database");

            foreach (var (address, balance) in INITIAL_BALANCES)
            {
                DatabaseService.SetBalance(address, balance);
                Console.WriteLine($"Set initial balance for {Convert.ToHexString(address).ToLowerInvariant()}: {balance}");
            }

            DatabaseService.Flush();
            Console.WriteLine("Initial balances setup completed");
        }
    }

    private static async Task SubscribeAndSync(ulong fromBlock)
    {
        Console.WriteLine($"Starting VIDA transaction subscription from block {fromBlock}");

        pwrClient = new RPC(RPC_URL);

        subscription = pwrClient.SubscribeToVidaTransactions(
            VIDA_ID,
            fromBlock,
            ProcessTransaction
        );

        Console.WriteLine($"Successfully subscribed to VIDA {VIDA_ID} transactions");

        StartBlockProgressMonitor(fromBlock);
        Console.WriteLine("Block progress monitor started");
    }

    private static void StartBlockProgressMonitor(ulong startBlock)
    {
        ulong lastChecked = startBlock;

        blockProgressMonitor = new Timer(async _ =>
        {
            try
            {
                var currentBlock = subscription?.GetLatestCheckedBlock() ?? DatabaseService.GetLastCheckedBlock();

                if (currentBlock > lastChecked)
                {
                    await OnChainProgress(currentBlock);
                    lastChecked = currentBlock;
                }
            }
            catch (Exception error)
            {
                Console.WriteLine($"Error in block progress monitor: {error.Message}");
            }
        }, null, TimeSpan.Zero, TimeSpan.FromSeconds(5));
    }

    private static async Task OnChainProgress(ulong blockNumber)
    {
        DatabaseService.SetLastCheckedBlock(blockNumber);
        await CheckRootHashValidityAndSave(blockNumber);
        Console.WriteLine($"Checkpoint updated to block {blockNumber}");
        DatabaseService.Flush();
    }

    private static void ProcessTransaction(VidaDataTransaction txn)
    {
        Console.WriteLine($"TRANSACTION RECEIVED: {txn.Data}");

        try
        {
            HandleTransaction(txn);
        }
        catch (Exception error)
        {
            Console.WriteLine($"Error processing transaction: {error.Message}");
        }
    }

    private static void HandleTransaction(VidaDataTransaction txn)
    {
        var dataBytes = PWR.Utils.Extensions.HexStringToByteArray(txn.Data);
        var dataStr = Encoding.UTF8.GetString(dataBytes);
        var jsonData = JObject.Parse(dataStr);

        var action = jsonData["action"]?.ToString() ?? "";

        if (action.Equals("transfer", StringComparison.OrdinalIgnoreCase))
        {
            HandleTransfer(jsonData, txn.Sender);
        }
    }

    private static void HandleTransfer(JObject jsonData, string senderHex)
    {
        var amountToken = jsonData["amount"];
        BigInteger amount;

        if (amountToken?.Type == JTokenType.String)
        {
            amount = BigInteger.Parse(amountToken.ToString());
        }
        else if (amountToken?.Type == JTokenType.Integer)
        {
            amount = new BigInteger((long)amountToken);
        }
        else
        {
            Console.WriteLine("Invalid or missing amount");
            return;
        }

        var receiverHex = jsonData["receiver"]?.ToString();
        if (string.IsNullOrEmpty(receiverHex))
        {
            Console.WriteLine("Missing receiver");
            return;
        }

        var sender = DecodeHexAddress(senderHex);
        var receiver = DecodeHexAddress(receiverHex);

        var success = DatabaseService.Transfer(sender, receiver, amount);

        if (success)
        {
            Console.WriteLine($"Transfer succeeded: {amount} from {senderHex} to {receiverHex}");
        }
        else
        {
            Console.WriteLine($"Transfer failed (insufficient funds): {amount} from {senderHex} to {receiverHex}");
        }
    }

    private static byte[] DecodeHexAddress(string hexStr)
    {
        var cleanHex = hexStr.StartsWith("0x") ? hexStr[2..] : hexStr;
        return PWR.Utils.Extensions.HexStringToByteArray(cleanHex);
    }

    private static async Task CheckRootHashValidityAndSave(ulong blockNumber)
    {
        var localRoot = DatabaseService.GetRootHash();

        if (localRoot == null)
        {
            Console.WriteLine($"No local root hash available for block {blockNumber}");
            return;
        }

        int peersCount = peersToCheckRootHashWith.Count;
        int quorum = (peersCount * 2) / 3 + 1;
        int matches = 0;

        using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        foreach (var peer in peersToCheckRootHashWith)
        {
            var (success, peerRoot) = await FetchPeerRootHash(httpClient, peer, blockNumber);

            if (success && peerRoot != null)
            {
                if (localRoot.SequenceEqual(peerRoot))
                {
                    matches++;
                }
            }
            else
            {
                if (peersCount > 0)
                {
                    peersCount--;
                    quorum = (peersCount * 2) / 3 + 1;
                }
            }

            if (matches >= quorum)
            {
                DatabaseService.SetBlockRootHash(blockNumber, localRoot);
                Console.WriteLine($"Root hash validated and saved for block {blockNumber}");
                return;
            }
        }

        Console.WriteLine($"Root hash mismatch: only {matches}/{peersToCheckRootHashWith.Count} peers agreed");
        DatabaseService.RevertUnsavedChanges();
    }

    private static async Task<(bool success, byte[]? rootHash)> FetchPeerRootHash(
        HttpClient client, string peer, ulong blockNumber)
    {
        var url = $"http://{peer}/rootHash?blockNumber={blockNumber}";

        try
        {
            var response = await client.GetAsync(url);

            if (response.IsSuccessStatusCode)
            {
                var hexString = await response.Content.ReadAsStringAsync();
                var trimmed = hexString.Trim();

                if (string.IsNullOrEmpty(trimmed))
                {
                    Console.WriteLine($"Peer {peer} returned empty root hash for block {blockNumber}");
                    return (false, null);
                }

                try
                {
                    var rootHash = PWR.Utils.Extensions.HexStringToByteArray(trimmed);
                    Console.WriteLine($"Successfully fetched root hash from peer {peer} for block {blockNumber}");
                    return (true, rootHash);
                }
                catch (Exception)
                {
                    Console.WriteLine($"Invalid hex response from peer {peer} for block {blockNumber}");
                    return (false, null);
                }
            }
            else
            {
                Console.WriteLine($"Peer {peer} returned HTTP {response.StatusCode} for block {blockNumber}");
                return (true, null);
            }
        }
        catch (Exception)
        {
            Console.WriteLine($"Failed to fetch root hash from peer {peer} for block {blockNumber}");
            return (false, null);
        }
    }
}
