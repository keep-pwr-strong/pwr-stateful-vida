package api;

import io.pwrlabs.util.encoders.Hex;
import main.DatabaseService;

import static spark.Spark.get;

public class GET {
    /**
     * Initializes and registers all GET endpoint handlers with the Spark framework.
     * Currently registers the /rootHash endpoint for retrieving Merkle root hashes
     * for specific block numbers.
     */
    public static void run() {
        get("/rootHash", (request, response) -> {
            try {
                long blockNumber = Long.parseLong(request.queryParams("blockNumber"));

                if(blockNumber == DatabaseService.getLastCheckedBlock()) return Hex.toHexString(DatabaseService.getRootHash());
                else if(blockNumber < DatabaseService.getLastCheckedBlock() && blockNumber > 1) {
                    byte[] blockRootHash = DatabaseService.getBlockRootHash(blockNumber);
                    if (blockRootHash != null) {
                        return Hex.toHexString(blockRootHash);
                    } else {
                        response.status(400);
                        return "Block root hash not found for block number: " + blockNumber;
                    }
                } else {
                    response.status(400);
                    return "Invalid block number";
                }
            } catch (Exception e) {
                e.printStackTrace();
                return "";
            }
        });
    }
}
