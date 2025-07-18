using System;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using PWR;
using PWR.Models;
using PWR.Utils;

namespace PwrStatefulVIDA;

public class Handler
{
    // Constants
    private const ulong VIDA_ID = 73746238;
    private const string RPC_URL = "https://pwrrpc.pwrlabs.io/";

    private static RPC? pwrClient;
    private static VidaTransactionSubscription? subscription;
    // Global state
    public static List<string> peersToCheckRootHashWith = new();

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
        subscription.SetLatestCheckedBlock(DatabaseService.GetLastCheckedBlock());
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

        var senderAddress = senderHex.StartsWith("0x") ? senderHex[2..] : senderHex;
        var receiverAddress = receiverHex.StartsWith("0x") ? receiverHex[2..] : receiverHex;

        var sender = PWR.Utils.Extensions.HexStringToByteArray(senderAddress);
        var receiver = PWR.Utils.Extensions.HexStringToByteArray(receiverAddress);

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

    private static void ProcessTransaction(VidaDataTransaction txn)
    {
        try
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
        catch (Exception error)
        {
            Console.WriteLine($"Error processing transaction: {error.Message}");
        }
    }

    private static async Task OnChainProgress(ulong blockNumber)
    {
        DatabaseService.SetLastCheckedBlock(blockNumber);
        await CheckRootHashValidityAndSave(blockNumber);
        Console.WriteLine($"Checkpoint updated to block {blockNumber}");
        DatabaseService.Flush();
    }

    public static async Task SubscribeAndSync(ulong fromBlock)
    {
        Console.WriteLine($"Starting VIDA transaction subscription from block {fromBlock}");

        pwrClient = new RPC(RPC_URL);

        subscription = pwrClient.SubscribeToVidaTransactions(
            VIDA_ID,
            fromBlock,
            ProcessTransaction,
            OnChainProgress
        );
        Console.WriteLine($"Successfully subscribed to VIDA {VIDA_ID} transactions");
    }

}