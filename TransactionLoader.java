package metro;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class TransactionLoader implements Runnable {
    private final Queue<Map<String, String>> transactionStream;
    private final Map<String, Map<String, String>> hashTable;

    public TransactionLoader(Queue<Map<String, String>> transactionStream, Map<String, Map<String, String>> hashTable) {
        this.transactionStream = transactionStream;
        this.hashTable = hashTable;
    }

    @Override
    public void run() {
        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\786\\Downloads\\transactions.csv"))) {
            String line;
            // Skip header
            br.readLine();

            while ((line = br.readLine()) != null && !Thread.currentThread().isInterrupted()) {
                String[] values = line.split(",");
                if (values.length < 5) {
                    System.err.println("Skipping invalid transaction line: " + line);
                    continue;
                }
                Map<String, String> transaction = new HashMap<>();
                transaction.put("Order_ID", values[0]);
                transaction.put("Order_Date", values[1]);
                transaction.put("Product_ID", values[2]);
                transaction.put("Quantity", values[3]);
                transaction.put("Customer_ID", values[4]);

                // Store in hash table for MESHJOIN
                hashTable.put(transaction.get("Customer_ID"), transaction);
                transactionStream.offer(transaction);
            //    System.out.println("Loaded Transaction: " + transaction);
            }
        } catch (IOException e) {
            System.err.println("Transaction Loader Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Signal end of input
            MeshJoinETL.running = false; // Set running to false when done reading
        }
    }
}
