package metro;

import java.util.*;
import java.util.concurrent.*;

public class MeshJoinETL {
    static volatile boolean running = true; // Flag to control the running state
    private static final int PARTITION_SIZE = 100; // partition size
    private static final int BUFFER_SIZE = 100; // Size of the disk buffer

    public static void main(String[] args) {
        BlockingQueue<Map<String, String>> transactionStream = new LinkedBlockingQueue<>();
        BlockingQueue<Map<String, String>> enrichedDataQueue = new LinkedBlockingQueue<>();
        Map<String, Map<String, String>> hashTable = new ConcurrentHashMap<>(); // ConcurrentHashMap for thread-safe hash table

        // Load master data from CSV files
        List<Map<String, String>> productData = MasterDataLoader.loadProductData();
        List<Map<String, String>> customerData = MasterDataLoader.loadCustomerData();

        // Start ETL threads
        DataLoader dataLoader = new DataLoader(); // Create DataLoader instance

        // Start ETL threads
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        // Transaction Loader (producer)
        executorService.execute(new TransactionLoader(transactionStream, hashTable));

        // Mesh Join Transformer (processor)
        executorService.execute(new MeshJoinTransformer(transactionStream, enrichedDataQueue, hashTable, productData, customerData, PARTITION_SIZE, BUFFER_SIZE, dataLoader)); // Pass DataLoader

        // Data Loader (consumer)
        executorService.execute(() -> {
            while (MeshJoinETL.isRunning() || !transactionStream.isEmpty()) {
                try {
                    Map<String, String> transaction = transactionStream.take(); // Blocks if the queue is empty
                    if (!transaction.isEmpty()) {
                    //    System.out.println("Transaction Stream: " + transaction); // Print transaction stream
                        dataLoader.insertEnrichedTransaction(transaction); // Process the transaction
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("DataLoader thread interrupted: " + e.getMessage());
                } catch (Exception e) {
                   // System.err.println("Error while processing the transaction: " + e.getMessage());
                //    e.printStackTrace();
                }
            }
        });

        // Wait for processing to finish
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                System.err.println("Executor service did not terminate in the expected time.");
            }
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted while waiting for thread termination: " + e.getMessage());
            Thread.currentThread().interrupt(); // Restore the interrupt status
        }

        // Signal threads to stop
        running = false;
    }

    public static boolean isRunning() {
        return running;
    }
}
