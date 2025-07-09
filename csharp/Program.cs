using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json.Linq;
using PWR;
using PWR.Models;
using PWR.Utils;

namespace PwrStatefulVIDA;

/// <summary>
/// Main application class that orchestrates the entire system.
/// C# equivalent of the Rust Main struct.
/// </summary>
public class Main
{
    private readonly ulong _vidaId;
    private readonly ulong _startBlock;
    private readonly string _rpcUrl;
    private readonly int _defaultPort;
    private readonly int _requestTimeoutSecs;

    // Initial balances for fresh database
    private readonly Dictionary<byte[], BigInteger> _initialBalances;

    // Instance variables
    private RPC? _pwrClient;
    private List<string> _peersToCheckRootHashWith;
    private int _port;
    private WebApplication? _app;
    private VidaTransactionSubscription? _subscription;
    private Timer? _blockProgressMonitor;

    public Main()
    {
        _vidaId = 73746238;
        _startBlock = 1;
        _rpcUrl = "https://pwrrpc.pwrlabs.io/";
        _defaultPort = 8080;
        _requestTimeoutSecs = 10;

        // Set up initial balances (equivalent to Rust INITIAL_BALANCES)
        _initialBalances = new Dictionary<byte[], BigInteger>
        {
            { Convert.FromHexString("c767ea1d613eefe0ce1610b18cb047881bafb829"), new BigInteger(1000000000000) },
            { Convert.FromHexString("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4"), new BigInteger(1000000000000) },
            { Convert.FromHexString("9282d39ca205806473f4fde5bac48ca6dfb9d300"), new BigInteger(1000000000000) },
            { Convert.FromHexString("E68191B7913E72E6F1759531FBFAA089FF02308A"), new BigInteger(1000000000000) }
        };

        _peersToCheckRootHashWith = new List<string>();
        _port = 8080;
    }

    /// <summary>
    /// Parse command line arguments (equivalent to Rust parse_command_line_args)
    /// </summary>
    private void ParseCommandLineArgs(string[] args)
    {
        // Parse port if provided (simple implementation)
        if (args.Length > 0 && int.TryParse(args[0], out int port))
        {
            _port = port;
            Console.WriteLine($"Using port from args: {_port}");
        }

        // Parse peers (everything after port or from index 0)
        int peerStart = (args.Length > 0 && int.TryParse(args[0], out _)) ? 1 : 0;
        if (args.Length > peerStart)
        {
            _peersToCheckRootHashWith = new List<string>(args[peerStart..]);
        }
    }

    /// <summary>
    /// Sets up the initial account balances when starting from a fresh database.
    /// Equivalent to Rust init_initial_balances() method.
    /// </summary>
    private async Task InitInitialBalances()
    {
        var lastCheckedBlock = DatabaseService.GetLastCheckedBlock();

        if (lastCheckedBlock == 0)
        {
            Console.WriteLine("Setting up initial balances for fresh database");

            foreach (var (address, balance) in _initialBalances)
            {
                DatabaseService.SetBalance(address, balance);
                Console.WriteLine($"Set initial balance for {Convert.ToHexString(address).ToLowerInvariant()}: {balance}");
            }

            // Flush to ensure balances are persisted
            DatabaseService.Flush();
            Console.WriteLine("Initial balances setup completed");
        }
        else
        {
            Console.WriteLine($"Database already initialized. Resuming from block {lastCheckedBlock}");
        }
    }

    /// <summary>
    /// Initializes peer list from arguments or defaults.
    /// Equivalent to Rust initialize_peers() method.
    /// </summary>
    private void InitializePeers()
    {
        if (_peersToCheckRootHashWith.Count == 0)
        {
            _peersToCheckRootHashWith = new List<string> { "localhost:8080" };
            Console.WriteLine($"Using default peers: [{string.Join(", ", _peersToCheckRootHashWith)}]");
        }
        else
        {
            Console.WriteLine($"Using peers from args: [{string.Join(", ", _peersToCheckRootHashWith)}]");
        }
    }

    /// <summary>
    /// Start the API server
    /// Equivalent to Rust start_api_server() method.
    /// </summary>
    private async Task StartApiServer()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{_port}");
        
        _app = builder.Build();
        GET.Run(_app);

        // Start server in background
        _ = Task.Run(async () =>
        {
            Console.WriteLine($"Starting API server on port {_port}");
            await _app.RunAsync();
        });

        // Give server time to start
        await Task.Delay(2000);
        Console.WriteLine($"API server started on http://0.0.0.0:{_port}");
    }

    /// <summary>
    /// Subscribes to VIDA transactions starting from the given block.
    /// Equivalent to Rust subscribe_and_sync() method.
    /// </summary>
    private async Task SubscribeAndSync(ulong fromBlock)
    {
        Console.WriteLine($"Starting VIDA transaction subscription from block {fromBlock}");

        // Initialize RPC client
        _pwrClient = new RPC(_rpcUrl);

        // Subscribe to VIDA transactions using PWR.NET (like Rust pwr_rs)
        _subscription = _pwrClient.SubscribeToVidaTransactions(
            _vidaId,
            fromBlock,
            ProcessTransaction // Transaction handler callback
        );

        Console.WriteLine($"Successfully subscribed to VIDA {_vidaId} transactions");

        // Start monitoring loop for block progress (equivalent to Rust _start_block_progress_monitor)
        StartBlockProgressMonitor(fromBlock);

        Console.WriteLine("Block progress monitor started");
    }

    /// <summary>
    /// Start block progress monitoring (equivalent to Rust tokio::spawn block)
    /// </summary>
    private void StartBlockProgressMonitor(ulong startBlock)
    {
        ulong lastChecked = startBlock;

        _blockProgressMonitor = new Timer(async _ =>
        {
            try
            {
                // Get current latest checked block from subscription (like Rust)
                var currentBlock = GetCurrentProcessedBlock();

                // If block has progressed, trigger validation
                if (currentBlock > lastChecked)
                {
                    try
                    {
                        await OnChainProgress(currentBlock, _peersToCheckRootHashWith);
                        lastChecked = currentBlock;
                    }
                    catch (Exception error)
                    {
                        Console.WriteLine($"Error in chain progress: {error.Message}");
                    }
                }
            }
            catch (Exception error)
            {
                Console.WriteLine($"Error in block progress monitor: {error.Message}");
            }
        }, null, TimeSpan.Zero, TimeSpan.FromSeconds(5)); // Check every 5 seconds like Rust
    }

    /// <summary>
    /// Get current processed block (simulates Rust subscription.get_latest_checked_block())
    /// </summary>
    private ulong GetCurrentProcessedBlock()
    {
        // In a real implementation, this would come from the subscription
        return _subscription?.GetLatestCheckedBlock() ?? DatabaseService.GetLastCheckedBlock();
    }

    /// <summary>
    /// Callback invoked as blocks are processed.
    /// Equivalent to Rust on_chain_progress() method.
    /// </summary>
    private static async Task OnChainProgress(ulong blockNumber, List<string> peers)
    {
        try
        {
            DatabaseService.SetLastCheckedBlock(blockNumber);
            await CheckRootHashValidityAndSave(blockNumber, peers);
            Console.WriteLine($"Checkpoint updated to block {blockNumber}");

            // Flush changes to disk after each checkpoint like Rust
            DatabaseService.Flush();
        }
        catch (Exception error)
        {
            Console.WriteLine($"Error in chain progress for block {blockNumber}: {error.Message}");
            throw;
        }
    }

    /// <summary>
    /// Processes a single VIDA transaction.
    /// Equivalent to Rust process_transaction() method.
    /// </summary>
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

    /// <summary>
    /// Handle transaction processing
    /// </summary>
    private static void HandleTransaction(VidaDataTransaction txn)
    {
        // Get transaction data and convert from hex to bytes (like Rust)
        var dataBytes = PWR.Utils.Extensions.HexStringToByteArray(txn.Data);

        // Parse JSON data
        var dataStr = Encoding.UTF8.GetString(dataBytes);
        var jsonData = JObject.Parse(dataStr);

        // Get action from JSON
        var action = jsonData["action"]?.ToString() ?? "";

        if (action.Equals("transfer", StringComparison.OrdinalIgnoreCase))
        {
            HandleTransfer(jsonData, txn.Sender);
        }
        else
        {
            Console.WriteLine($"Ignoring transaction with action: {action}");
        }
    }

    /// <summary>
    /// Executes a token transfer described by the given JSON payload.
    /// Equivalent to Rust handle_transfer() method.
    /// </summary>
    private static void HandleTransfer(JObject jsonData, string senderHex)
    {
        // Extract amount and receiver from JSON
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
            throw new ArgumentException("Invalid or missing amount");
        }

        var receiverHex = jsonData["receiver"]?.ToString();
        if (string.IsNullOrEmpty(receiverHex))
        {
            throw new ArgumentException("Missing receiver");
        }

        // Decode hex addresses
        var sender = DecodeHexAddress(senderHex);
        var receiver = DecodeHexAddress(receiverHex);

        // Execute transfer
        var success = DatabaseService.Transfer(sender, receiver, amount);

        if (success)
        {
            Console.WriteLine($"Transfer succeeded: {amount} from {senderHex} to {receiverHex}");
        }
        else
        {
            Console.WriteLine($"Transfer failed (insufficient funds): {jsonData}");
        }
    }

    /// <summary>
    /// Decodes a hexadecimal address into raw bytes.
    /// Equivalent to Rust decode_hex_address() method.
    /// </summary>
    private static byte[] DecodeHexAddress(string hexStr)
    {
        // Remove '0x' prefix if present
        var cleanHex = hexStr.StartsWith("0x") ? hexStr[2..] : hexStr;
        return PWR.Utils.Extensions.HexStringToByteArray(cleanHex);
    }

    /// <summary>
    /// Validates the local Merkle root against peers and persists it if a quorum
    /// of peers agree. Equivalent to Rust check_root_hash_validity_and_save() method.
    /// </summary>
    private static async Task CheckRootHashValidityAndSave(ulong blockNumber, List<string> peers)
    {
        var localRoot = DatabaseService.GetRootHash();

        if (localRoot == null)
        {
            Console.WriteLine($"No local root hash available for block {blockNumber}");
            return;
        }

        int peersCount = peers.Count;
        int quorum = (peersCount * 2) / 3 + 1;
        int matches = 0;

        using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        foreach (var peer in peers)
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

        Console.WriteLine($"Root hash mismatch: only {matches}/{peers.Count} peers agreed");

        // Revert changes and reset block to reprocess the data
        DatabaseService.RevertUnsavedChanges();
        // Note: In real implementation, you'd reset the subscription
    }

    /// <summary>
    /// Fetches the root hash from a peer node for the specified block number.
    /// Equivalent to Rust fetch_peer_root_hash() method.
    /// </summary>
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
                return (true, null); // Peer responded but with error
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to fetch root hash from peer {peer} for block {blockNumber}: {ex.Message}");
            return (false, null);
        }
    }

    /// <summary>
    /// Main application entry point that orchestrates the entire system.
    /// Equivalent to Rust run() method.
    /// </summary>
    public async Task Run(string[] args)
    {
        Console.WriteLine("=== Starting PWR VIDA Synchronizer ===");

        try
        {
            // Parse command line arguments
            ParseCommandLineArgs(args);

            // Initialize database service
            DatabaseService.Initialize();

            // Start API server
            await StartApiServer();

            // Initialize initial balances
            await InitInitialBalances();

            // Initialize peers
            InitializePeers();

            // Determine starting block
            var lastBlock = DatabaseService.GetLastCheckedBlock();
            var fromBlock = lastBlock > 0 ? lastBlock : _startBlock;

            Console.WriteLine($"Starting synchronization from block {fromBlock}");

            // Subscribe and sync
            await SubscribeAndSync(fromBlock);

            // Keep the main thread alive
            Console.WriteLine("Application started successfully. Press Ctrl+C to exit.");

            // Set up graceful shutdown handlers
            SetupShutdownHandlers();

            // Keep process alive
            await KeepAlive();
        }
        catch (Exception error)
        {
            Console.WriteLine($"Application failed to start: {error.Message}");
            await Shutdown();
            Environment.Exit(1);
        }
    }

    /// <summary>
    /// Setup graceful shutdown handlers
    /// </summary>
    private void SetupShutdownHandlers()
    {
        Console.CancelKeyPress += async (sender, e) =>
        {
            e.Cancel = true;
            Console.WriteLine("\nReceived Ctrl+C. Shutting down gracefully...");
            await Shutdown();
            Environment.Exit(0);
        };

        AppDomain.CurrentDomain.ProcessExit += async (sender, e) =>
        {
            await Shutdown();
        };
    }

    /// <summary>
    /// Keep the process alive (equivalent to Rust's tokio::signal::ctrl_c().await)
    /// </summary>
    private async Task KeepAlive()
    {
        var tcs = new TaskCompletionSource<bool>();
        Console.CancelKeyPress += (sender, e) => tcs.SetResult(true);
        await tcs.Task;
    }

    /// <summary>
    /// Graceful application shutdown
    /// </summary>
    private async Task Shutdown()
    {
        Console.WriteLine("Shutting down application...");

        try
        {
            // Stop block progress monitor
            _blockProgressMonitor?.Dispose();
            _blockProgressMonitor = null;

            // Stop subscription
            _subscription?.Stop();
            _subscription = null;

            // Stop API server
            if (_app != null)
            {
                await _app.StopAsync();
                await _app.DisposeAsync();
                _app = null;
            }

            // Flush any pending database changes
            DatabaseService.Flush();
            Console.WriteLine("Flushed database changes");

            // Close database service
            DatabaseService.Close();
            Console.WriteLine("Closed database service");
        }
        catch (Exception error)
        {
            Console.WriteLine($"Error during shutdown: {error.Message}");
        }

        Console.WriteLine("Application shutdown complete");
    }
}

/// <summary>
/// Application entry point.
/// Equivalent to Rust main() function
/// </summary>
public class Program
{
    public static async Task Main(string[] args)
    {
        var app = new Main();
        await app.Run(args);
    }
}
