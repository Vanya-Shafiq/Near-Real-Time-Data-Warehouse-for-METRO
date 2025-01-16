package metro;
import java.util.Map;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.*;

class MeshJoinTransformer implements Runnable {
    private final Queue<Map<String, String>> transactionStream;
    private final Queue<Map<String, String>> enrichedDataQueue;
    private final Map<String, Map<String, String>> hashTable; // Hash table for transactions
    private final List<Map<String, String>> productData;
    private final List<Map<String, String>> customerData;
    private final int partitionSize;
    private final int bufferSize;

    // Define the date format for parsing
    private static final SimpleDateFormat inputDateFormat = new SimpleDateFormat("M/d/yyyy HH:mm");
    private static final SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private int currentProductPartitionIndex = 0; // To track current partition index for products
    private int currentCustomerPartitionIndex = 0;  // To track current partition index for customers

    private final DataLoader dataLoader; 

    public MeshJoinTransformer(Queue<Map<String, String>> transactionStream,
                               Queue<Map<String, String>> enrichedDataQueue,
                               Map<String, Map<String, String>> hashTable,
                               List<Map<String, String>> productData,
                               List<Map<String, String>> customerData,
                               int partitionSize,
                               int bufferSize,
                               DataLoader dataLoader) { 
        this.transactionStream = transactionStream;
        this.enrichedDataQueue = enrichedDataQueue;
        this.hashTable = hashTable;
        this.productData = productData;
        this.customerData = customerData;
        this.partitionSize = partitionSize;
        this.bufferSize = bufferSize;
        this.dataLoader = dataLoader; // Initialize DataLoader
    }

    @Override
    public void run() {
        while (MeshJoinETL.isRunning() || !transactionStream.isEmpty()) {
            // Load the current partitions of products and customers into memory
            List<Map<String, String>> productPartition = loadMDPartition(productData, currentProductPartitionIndex);
            List<Map<String, String>> customerPartition = loadMDPartition(customerData, currentCustomerPartitionIndex);

         // Process transactions in the queue
            while (!transactionStream.isEmpty()) {
                Map<String, String> transaction = transactionStream.poll();
                if (transaction != null) {
                    // Store transaction in hash table
                    hashTable.put(transaction.get("Customer_ID"), transaction);
                    // Join and enrich the transaction
                    if (joinAndEnrich(transaction, productPartition, customerPartition)) {
                        enrichedDataQueue.offer(transaction);
                        dataLoader.insertEnrichedTransaction(transaction); // Insert into DB
                        printFilteredTransaction(transaction); // Print only the required fields
                    }
                }
            }

            // Remove the oldest chunk from the hash table
            if (!transactionStream.isEmpty()) {
                String oldestCustomerId = transactionStream.peek().get("Customer_ID");
                hashTable.remove(oldestCustomerId);
            }

            // Move to the next partition
            currentProductPartitionIndex = (currentProductPartitionIndex + 1) % (productData.size() / partitionSize);
            currentCustomerPartitionIndex = (currentCustomerPartitionIndex + 1) % (customerData.size() / partitionSize);

            try {
                Thread.sleep(100); // Simulate processing delay
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt(); // Graceful exit on interruption
            }
        }
    }

    private List<Map<String, String>> loadMDPartition(List<Map<String, String>> data, int partitionIndex) {
        int start = partitionIndex * partitionSize;
        int end = Math.min(start + partitionSize, data.size());
        return data.subList(start, end);
    }

    private boolean joinAndEnrich(Map<String, String> transaction, List<Map<String, String>> productPartition, List<Map<String, String>> customerPartition) {
        String productId = transaction.get("Product_ID");
        String customerId = transaction.get("Customer_ID");
        String orderDate = transaction.get("Order_Date");

        // Enrich transaction with product data
        for (Map<String, String> product : productPartition) {
            if (product.get("Product_ID").equals(productId)) {
                transaction.put("Product_Name", product.get("Product_Name"));
                transaction.put("Product_Price", product.get("Product_Price"));
                transaction.put("Supplier_ID", product.get("Supplier_ID"));
                transaction.put("Supplier_Name", product.get("Supplier_Name"));
                transaction.put("Store_ID", product.get("Store_ID")); 
                transaction.put("Store_Name", product.get("Store_Name")); 
                break;
            }
        }

        // Enrich transaction with customer data
        for (Map<String, String> customer : customerPartition) {
            if (customer.get("Customer_ID").equals(customerId)) {
                transaction.put("Customer_Name", customer.get("Customer_Name"));
                transaction.put("Gender", customer.get("Gender"));
                break;
            }
        }

        // Ensure date fields are populated
        if (orderDate != null) {
            SimpleDateFormat inputDateFormat = new SimpleDateFormat("M/d/yyyy HH:mm");
            try {
                Date date = inputDateFormat.parse(orderDate);
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);

                transaction.put("Full_Date", outputDateFormat.format(date)); // Store formatted date
                transaction.put("Day", String.valueOf(cal.get(Calendar.DAY_OF_MONTH)));
                transaction.put("Month", String.valueOf(cal.get(Calendar.MONTH) + 1));
                transaction.put("Quarter", String.valueOf((cal.get(Calendar.MONTH) / 3) + 1));
                transaction.put("Year", String.valueOf(cal.get(Calendar.YEAR)));
                transaction.put("Season", getSeason(cal.get(Calendar.MONTH)));
                transaction.put("Weekday_Indicator", (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) ? "E" : "W");

            } catch (ParseException e) {
               // System.err.println("Error parsing order date: " + orderDate);
                //e.printStackTrace();
            }
        }

        // Calculate SALE
        String quantityStr = transaction.get("Quantity");
        String productPriceStr = transaction.get("Product_Price");

        if (quantityStr != null && productPriceStr != null) {
            try {
                productPriceStr = productPriceStr.replace("$", "").trim();
                int quantity = Integer.parseInt(quantityStr);
                double productPrice = Double.parseDouble(productPriceStr);
                double sale = quantity * productPrice;
                transaction.put("SALE", String.valueOf(sale)); // Store the calculated sale
            } catch (NumberFormatException e) {
              //  System.err.println("Error parsing quantity or product price for Order ID: " + transaction.get("Order_ID"));
                transaction.put("SALE", "0.0"); // Set default value on error
            }
        } else {
            transaction.put("SALE", "0.0"); // Set default value if quantity or price is missing
        }

        // Ensure all required fields are present
        return transaction.containsKey("Order_ID") && transaction.containsKey("Customer_ID") &&
               transaction.containsKey("Product_ID") && transaction.containsKey("Quantity") &&
               transaction.containsKey("Full_Date") && transaction.containsKey("Day") &&
               transaction.containsKey("Month") && transaction.containsKey ("Quarter") && transaction.containsKey("Year") &&
               transaction.containsKey("Season") && transaction.containsKey("Weekday_Indicator") &&
               transaction.containsKey("Product_Name") && transaction.containsKey("Product_Price") &&
               transaction.containsKey("Supplier_ID") && transaction.containsKey("Supplier_Name") &&
               transaction.containsKey("Store_ID") && transaction.containsKey("Store_Name");
    }

    private void printFilteredTransaction(Map<String, String> transaction) {
        Map<String, String> filteredTransaction = new HashMap<>();
        filteredTransaction.put("ORDER_ID", transaction.get("Order_ID"));
        filteredTransaction.put("ORDER_DATE", transaction.get("Order_Date"));
        filteredTransaction.put("PRODUCT_ID", transaction.get("Product_ID"));
        filteredTransaction.put("CUSTOMER_ID", transaction.get("Customer_ID"));
        filteredTransaction.put("CUSTOMER_NAME", transaction.get("Customer_Name"));
        filteredTransaction.put("GENDER", transaction.get("Gender"));
        filteredTransaction.put("QUANTITY", transaction.get("Quantity"));
        filteredTransaction.put("PRODUCT_NAME", transaction.get("Product_Name"));
        filteredTransaction.put("PRODUCT_PRICE", transaction.get("Product_Price"));
        filteredTransaction.put("SUPPLIER_ID", transaction.get("Supplier_ID"));
        filteredTransaction.put("SUPPLIER_NAME", transaction.get("Supplier_Name"));
        filteredTransaction.put("STORE_ID", transaction.get("Store_ID"));
        filteredTransaction.put("STORE_NAME", transaction.get("Store_Name"));
        filteredTransaction.put("SALE", transaction.get("SALE"));

        System.out.println("Enriched Transaction: " + filteredTransaction);
    }

    private String getSeason(int month) {
        if (month >= 3 && month <= 5) return "Spring";
        if (month >= 6 && month <= 8) return "Summer";
        if (month >= 9 && month <= 11) return "Fall";
        return "Winter"; // December, January, February
    }
    
}

