using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;

namespace PwrStatefulVIDA;

/// <summary>
/// GET API handler class - equivalent to Rust's GET struct
/// </summary>
public static class GET
{
    /// <summary>
    /// Initializes and registers all GET endpoint handlers.
    /// Currently registers the /rootHash endpoint for retrieving Merkle root hashes
    /// for specific block numbers.
    /// </summary>
    public static void Run(WebApplication app)
    {
        app.MapGet("/rootHash", async (HttpContext context) =>
        {
            // Equivalent to Rust try-catch block
            try
            {
                var response = HandleRootHash(context.Request.Query);
                await context.Response.WriteAsync(response);
            }
            catch (Exception)
            {
                // Equivalent to Rust's Err(_e) => String::new()
                await context.Response.WriteAsync("");
            }
        });
    }

    private static string HandleRootHash(IQueryCollection queryParams)
    {
        // long blockNumber = Long.parseLong(request.queryParams("blockNumber"));
        if (!queryParams.TryGetValue("blockNumber", out var blockNumberStr) || 
            !ulong.TryParse(blockNumberStr, out var blockNumber))
        {
            throw new ArgumentException("Missing or invalid blockNumber parameter");
        }

        var lastCheckedBlock = DatabaseService.GetLastCheckedBlock();

        // if(blockNumber == DatabaseService.getLastCheckedBlock()) 
        // return Hex.toHexString(DatabaseService.getRootHash());
        if (blockNumber == lastCheckedBlock)
        {
            var rootHash = DatabaseService.GetRootHash();
            return rootHash != null ? Convert.ToHexString(rootHash).ToLowerInvariant() : "";
        }
        // else if(blockNumber < DatabaseService.getLastCheckedBlock() && blockNumber > 1)
        else if (blockNumber < lastCheckedBlock && blockNumber > 1)
        {
            // byte[] blockRootHash = DatabaseService.getBlockRootHash(blockNumber);
            var blockRootHash = DatabaseService.GetBlockRootHash(blockNumber);
            
            // if (blockRootHash != null) {
            //     return Hex.toHexString(blockRootHash);
            // } else {
            //     return "Block root hash not found for block number: " + blockNumber;
            // }
            return blockRootHash != null 
                ? Convert.ToHexString(blockRootHash).ToLowerInvariant()
                : $"Block root hash not found for block number: {blockNumber}";
        }
        else
        {
            // return "Invalid block number";
            return "Invalid block number";
        }
    }
}
