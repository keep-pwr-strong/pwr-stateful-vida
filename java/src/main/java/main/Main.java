package main;

import api.GET;
import org.bouncycastle.util.encoders.Hex;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static spark.Spark.port;

/**
 * Entry point for synchronizing VIDA transactions with the local Merkle-backed database.
 */
public final class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final long START_BLOCK = 1L;
    private static final int PORT = 8080;
    public static List<String> peersToCheckRootHashWith;

    /**
     * Application entry point.
     *
     * @param args optional list of peer hosts to query for root hash
     */
    public static void main(String[] args) {
        try {
            port(PORT);
            GET.run();
            initInitialBalances();
            initializePeers(args);
            long lastBlock = DatabaseService.getLastCheckedBlock();
            long fromBlock = (lastBlock > 0) ? lastBlock : START_BLOCK;
            Handler.subscribeAndSync(fromBlock);
        } catch (IOException | RocksDBException e) {
            LOGGER.log(Level.SEVERE, "Initialization failed", e);
        }
    }

    /**
     * Sets up the initial account balances when starting from a fresh database.
     *
     * @throws RocksDBException if persisting the balances fails
     */
    private static void initInitialBalances() throws RocksDBException {
        if(DatabaseService.getLastCheckedBlock() == 0) {
            DatabaseService.setBalance(Hex.decode("c767ea1d613eefe0ce1610b18cb047881bafb829"), BigInteger.valueOf(1_0000_000_000_000L));
            DatabaseService.setBalance(Hex.decode("3b4412f57828d1ceb0dbf0d460f7eb1f21fed8b4"), BigInteger.valueOf(1_0000_000_000_000L));
            DatabaseService.setBalance(Hex.decode("9282d39ca205806473f4fde5bac48ca6dfb9d300"), BigInteger.valueOf(1_0000_000_000_000L));
            DatabaseService.setBalance(Hex.decode("e68191b7913e72e6f1759531fbfaa089ff02308a"), BigInteger.valueOf(1_0000_000_000_000L));
        }
    }

    /**
     * Initializes peer list from arguments or defaults.
     * @param args command-line arguments; if present, each arg is a peer hostname
     */
    private static void initializePeers(String[] args) {
        if (args != null && args.length > 0) {
            peersToCheckRootHashWith = Arrays.asList(args);
            LOGGER.info("Using peers from args: " + peersToCheckRootHashWith);
        } else {
            peersToCheckRootHashWith = List.of(
                    "localhost:8080"
            );
            LOGGER.info("Using default peers: " + peersToCheckRootHashWith);
        }
    }
}