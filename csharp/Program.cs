using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using PWR;
using PWR.Models;
using PWR.Utils;

namespace PwrStatefulVIDA;

public class Program
{
    // Constants
    private const ulong START_BLOCK = 1;
    private const int PORT = 8080;
    
    // Initial balances for fresh database
    private static readonly Dictionary<byte[], BigInteger> INITIAL_BALANCES = new()
    {
        { Convert.FromHexString("c767ea1d613eefe0ce1610b18cb047881bafb829"), new BigInteger(1000000000000) },
        { Convert.FromHexString("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4"), new BigInteger(1000000000000) },
        { Convert.FromHexString("9282d39ca205806473f4fde5bac48ca6dfb9d300"), new BigInteger(1000000000000) },
        { Convert.FromHexString("e68191b7913e72e6f1759531fbfaa089ff02308a"), new BigInteger(1000000000000) },
    };

    private static void InitializePeers(string[] args)
    {
        if (args.Length > 0)
        {
            Handler.peersToCheckRootHashWith = args.ToList();
            Console.WriteLine($"Using peers from args: [{string.Join(", ", Handler.peersToCheckRootHashWith)}]");
        }
        else
        {
            Handler.peersToCheckRootHashWith = new List<string> { "localhost:8080" };
            Console.WriteLine($"Using default peers: [{string.Join(", ", Handler.peersToCheckRootHashWith)}]");
        }
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
            Console.WriteLine("Initial balances setup completed");
        }
    }

    private static async Task StartApiServer()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{PORT}");
        
        // Disable ASP.NET Core request logging
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        
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
        
        await Handler.SubscribeAndSync(fromBlock);
        
        // Keep the main thread alive
        Console.WriteLine("Application started successfully. Press Ctrl+C to exit.");
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            Environment.Exit(0);
        };
        await Task.Delay(Timeout.Infinite);
    }
}
