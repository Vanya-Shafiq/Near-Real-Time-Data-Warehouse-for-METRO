package metro;

import java.util.*;
import java.util.concurrent.*;

public class Transformer implements Runnable {
    private final BlockingQueue<ConcurrentHashMap<String, String>> transactionStream;
    private final DataLoader dataLoader; // Reference to DataLoader for immediate insertion
    private final List<Map<String, String>> productData;
    private final List<Map<String, String>> customerData;

    public Transformer(BlockingQueue<ConcurrentHashMap<String, String>> transactionStream,
                       DataLoader dataLoader,
                       List<Map<String, String>> productData,
                       List<Map<String, String>> customerData) {
        this.transactionStream = transactionStream;
        this.dataLoader = dataLoader; 
        this.productData = productData;
        this.customerData = customerData;
    }

    @Override
    public void run() {
        while (MeshJoinETL.isRunning() || !transactionStream.isEmpty()) {
            try {
                // Use take() to block until a transaction is available
                ConcurrentHashMap<String, String> transaction = transactionStream.take();
                if (transaction != null) {
                    enrichTransaction(transaction);
                    dataLoader.insertEnrichedTransaction(transaction); // Insert immediately
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Graceful exit on interruption
               // System.err.println("Transformer thread interrupted: " + e.getMessage());
            }
        }
    }

    private void enrichTransaction(ConcurrentHashMap<String, String> transaction) {
        String productId = transaction.get("Product_ID");
        String customerId = transaction.get("Customer_ID");

        // Enrich transaction with product data
        for (Map<String, String> product : productData) {
            if (product.get("Product_ID").equals(productId)) {
                transaction.put("Product_Name", product.get("Product_Name"));
                transaction.put("Product_Price", product.get("Product_Price"));
                break;
            }
        }

        // Enrich transaction with customer data
        for (Map<String, String> customer : customerData) {
            if (customer.get("Customer_ID").equals(customerId)) {
                transaction.put("Customer_Name", customer.get("Customer_Name"));
                transaction.put("Gender", customer.get("Gender").trim());
                break;
            }
        }

        // Calculate SALE
        String quantityStr = transaction.get("Quantity");
        String productPriceStr = transaction.get("Product_Price");

        if (quantityStr != null && productPriceStr != null) {
            try {
                int quantity = Integer.parseInt(quantityStr);
                double productPrice = Double.parseDouble(productPriceStr);
                double sale = quantity * productPrice;
                transaction.put("SALE", String.valueOf(sale)); // Store the calculated sale
            } catch (NumberFormatException e) {
                transaction.put("SALE", "0.0"); // Set default value on error
            }
        } else {
            transaction.put("SALE", "0.0"); // Set default value if quantity or price is missing
        }

        // Add additional fields if necessary
        transaction.put("ORDER_ID", transaction.get("Order_ID"));
        transaction.put("ORDER_DATE", transaction.get("Order_Date"));
        transaction.put("SUPPLIER_ID", ""); 
        transaction.put("SUPPLIER_NAME", ""); 
        transaction.put("STORE_ID", ""); 
        transaction.put("STORE_NAME", ""); 
        }
}