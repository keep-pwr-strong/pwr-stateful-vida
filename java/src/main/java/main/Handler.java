package main;

import com.github.pwrlabs.pwrj.entities.FalconTransaction;
import com.github.pwrlabs.pwrj.protocol.PWRJ;
import com.github.pwrlabs.pwrj.protocol.VidaTransactionSubscription;
import io.pwrlabs.util.encoders.BiResult;
import org.bouncycastle.util.encoders.Hex;
import org.json.JSONObject;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Handler {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final long VIDA_ID = 73_746_238L;
    private static final PWRJ PWRJ_CLIENT = new PWRJ("https://pwrrpc.pwrlabs.io/");
    private static VidaTransactionSubscription subscription;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Subscribes to VIDA transactions starting from the given block.
     *
     * @param fromBlock block height to begin synchronization from
     * @throws IOException if network communication fails
     * @throws RocksDBException if persisting data fails
     */
    public static void subscribeAndSync(long fromBlock) throws IOException, RocksDBException {
        //The subscription to VIDA transactions has a built in shutdwown hook
        subscription =
                PWRJ_CLIENT.subscribeToVidaTransactions(
                        PWRJ_CLIENT,
                        VIDA_ID,
                        fromBlock,
                        Handler::onChainProgress,
                        Handler::processTransaction
                );
    }

    /**
     * Callback invoked as blocks are processed.
     *
     * @param blockNumber block height that was just processed
     * @return always {@code null}
     */
    private static Void onChainProgress(long blockNumber) {
        try {
            DatabaseService.setLastCheckedBlock(blockNumber);
            checkRootHashValidityAndSave(blockNumber);
            LOGGER.info("Checkpoint updated to block " + blockNumber);
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to update last checked block: " + blockNumber, e);
        } finally {
            return null;
        }
    }

    /**
     * Processes a single VIDA transaction.
     *
     * @param txn the transaction to handle
     */
    private static void processTransaction(FalconTransaction.PayableVidaDataTxn txn) {
        try {
            JSONObject json = new JSONObject(new String(txn.getData(), StandardCharsets.UTF_8));
            String action = json.optString("action", "");
            if ("transfer".equalsIgnoreCase(action)) {
                handleTransfer(json, txn.getSender());
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error processing transaction: " + txn.getTransactionHash(), e);
        }
    }

    /**
     * Executes a token transfer described by the given JSON payload.
     *
     * @param json       transfer description
     * @param senderHex  hexadecimal sender address
     * @throws RocksDBException if balance updates fail
     */
    private static void handleTransfer(JSONObject json, String senderHex) throws RocksDBException {
        BigInteger amount = json.optBigInteger("amount", null);
        String receiverHex = json.optString("receiver", null);
        if (amount == null || receiverHex == null) {
            LOGGER.warning("Skipping invalid transfer: " + json);
            return;
        }

        byte[] sender = decodeHexAddress(senderHex);
        byte[] receiver = decodeHexAddress(receiverHex);

        boolean success = DatabaseService.transfer(sender, receiver, amount);
        if (success) {
            LOGGER.info("Transfer succeeded: " + json);
        } else {
            LOGGER.warning("Transfer failed (insufficient funds): " + json);
        }
    }

    /**
     * Decodes a hexadecimal address into raw bytes.
     *
     * @param hex hexadecimal string, optionally prefixed with {@code 0x}
     * @return 20-byte address
     */
    private static byte[] decodeHexAddress(String hex) {
        String clean = hex.startsWith("0x") ? hex.substring(2) : hex;
        return Hex.decode(clean);
    }

    /**
     * Validates the local Merkle root against peers and persists it if a quorum
     * of peers agree.
     *
     * @param blockNumber block height being validated
     */
    private static void checkRootHashValidityAndSave(long blockNumber) {
        try {
            byte[] localRoot = DatabaseService.getRootHash();
            int peersCount = Main.peersToCheckRootHashWith.size();
            long quorum = (peersCount * 2) / 3 + 1;
            int matches = 0;
            for (String peer : Main.peersToCheckRootHashWith) {
                // TODO: fetch peer root via RPC and compare
                BiResult<Boolean /**/, byte[]> response = fetchPeerRootHash(peer, blockNumber);
                if(response.getFirst()) {
                    if(Arrays.equals(response.getSecond(), localRoot)) {
                        matches++;
                    }
                } else {
                    --peersCount;
                    quorum = (peersCount * 2) / 3 + 1;
                }

                if (matches >= quorum) {
                    DatabaseService.setBlockRootHash(blockNumber, localRoot);
                    LOGGER.info("Root hash validated and saved for block " + blockNumber);
                    return;
                }
            }

            LOGGER.severe("Root hash mismatch: only " + matches + "/" + Main.peersToCheckRootHashWith.size());
            //Revert changes and reset block to reprocess the data
            DatabaseService.revertUnsavedChanges();
            subscription.setLatestCheckedBlock(DatabaseService.getLastCheckedBlock());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error verifying root hash at block " + blockNumber, e);
        }
    }

    /**
     * Fetches the root hash from a peer node for the specified block number.
     *
     * @param peer the peer hostname/address
     * @param blockNumber the block number to query
     * @return BiResult where first element indicates successful connection, second element contains the root hash bytes
     */
    private static BiResult<Boolean /*Replied*/, byte[]> fetchPeerRootHash(String peer, long blockNumber) {
        try {
            // Build the URL for the peer's rootHash endpoint
            String url = "http://" + peer + "/rootHash?blockNumber=" + blockNumber;

            // Create the HTTP request
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .timeout(REQUEST_TIMEOUT)
                    .header("Accept", "text/plain")
                    .build();

            // Send the request and get response
            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

            // Check if the response was successful
            if (response.statusCode() == 200) {
                String hexString = response.body().trim();

                // Validate that we received a non-empty hex string
                if (hexString.isEmpty()) {
                    LOGGER.warning("Peer " + peer + " returned empty root hash for block " + blockNumber);
                    return new BiResult<>(false, new byte[0]);
                }

                // Decode the hex string to bytes
                byte[] rootHash = Hex.decode(hexString);

                LOGGER.fine("Successfully fetched root hash from peer " + peer + " for block " + blockNumber);
                return new BiResult<>(true, rootHash);

            } else {
                LOGGER.warning("Peer " + peer + " returned HTTP " + response.statusCode() +
                        " for block " + blockNumber + ": " + response.body());
                return new BiResult<>(true, new byte[0]);
            }

        } catch (IllegalArgumentException e) {
            LOGGER.warning("Invalid hex response from peer " + peer + " for block " + blockNumber + ": " + e.getMessage());
            return new BiResult<>(false, new byte[0]);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to fetch root hash from peer " + peer + " for block " + blockNumber, e);
            return new BiResult<>(false, new byte[0]);
        }
    }
}
