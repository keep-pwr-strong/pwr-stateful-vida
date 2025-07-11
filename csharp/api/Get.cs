using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;

namespace PwrStatefulVIDA;

public static class GET
{
    public static void Run(WebApplication app)
    {
        app.MapGet("/rootHash", async (HttpContext context) =>
        {
            try
            {
                var response = HandleRootHash(context.Request.Query);
                await context.Response.WriteAsync(response);
            }
            catch (Exception)
            {
                await context.Response.WriteAsync("");
            }
        });
    }

    private static string HandleRootHash(IQueryCollection queryParams)
    {
        if (!queryParams.TryGetValue("blockNumber", out var blockNumberStr) || 
            !ulong.TryParse(blockNumberStr, out var blockNumber))
        {
            return "Missing or invalid blockNumber parameter";
        }

        var lastCheckedBlock = DatabaseService.GetLastCheckedBlock();

        if (blockNumber == lastCheckedBlock)
        {
            var rootHash = DatabaseService.GetRootHash();
            return rootHash != null ? Convert.ToHexString(rootHash).ToLowerInvariant() : "";
        }
        else if (blockNumber < lastCheckedBlock && blockNumber > 1)
        {
            var blockRootHash = DatabaseService.GetBlockRootHash(blockNumber);
            
            return blockRootHash != null 
                ? Convert.ToHexString(blockRootHash).ToLowerInvariant()
                : $"Block root hash not found for block number: {blockNumber}";
        }
        else
        {
            return "Invalid block number";
        }
    }
}
