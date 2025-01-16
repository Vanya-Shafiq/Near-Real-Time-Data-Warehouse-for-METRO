package metro;

import java.sql.*;
import java.util.*;
import java.math.BigDecimal;
import java.util.concurrent.*;

public class DataLoader {
    private static final String dbUrl = "jdbc:mysql://localhost:3307/metro";
    private static final String dbUser  = "root";
    private static final String dbPassword = "Charlie@112233";
    
    private int parseIntSafely(String input) {
        try {
            // Remove non-numeric characters and parse the number
            return Integer.parseInt(input.replaceAll("\\D", ""));
        } catch (NumberFormatException e) {
            // Return a default value or throw a custom exception as needed
            return -1;
        }
    }


    // Removed the static queue; we will directly insert transactions
    public void insertEnrichedTransaction(Map<String, String> transaction) {
        String insertCustomerSQL = "INSERT  INTO customers (Customer_ID, Customer_Name, Gender) VALUES (?, ?, ?)";
        String insertProductSQL = "INSERT  INTO products (Product_ID, Product_Name, Product_Price) VALUES (?, ?, ?)";
        String insertSalesSQL = "INSERT IGNORE INTO sales (Transaction_ID, Customer_ID, Product_ID, Store_ID,  Supplier_ID, Quantity, Price, Total_Sale) VALUES ( ?, ?, ?, ? , ?, ?, ?, ?)";
        String insertStoreSQL = "INSERT INTO stores (Store_ID, Store_Name) VALUES (?, ?)";
        String insertSupplierSQL = "INSERT  INTO suppliers (Supplier_ID, Supplier_Name) VALUES (?, ?)";
        String insertDateSQL = "INSERT  INTO date (Full_Date, Day, Month, Quarter, Year, Season, Weekday_Indicator) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            connection.setAutoCommit(false);

            try {
                // Insert Customers
                try (PreparedStatement customerStatement = connection.prepareStatement(insertCustomerSQL)) {
                    customerStatement.setInt(1, Integer.parseInt(transaction.get("Customer_ID")));
                    customerStatement.setString(2, transaction.get("Customer_Name"));
                    customerStatement.setString(3, transaction.get("Gender"));
                    customerStatement.executeUpdate();
                }

                // Insert Products
                try (PreparedStatement productStatement = connection.prepareStatement(insertProductSQL)) {
                    productStatement.setInt(1, Integer.parseInt(transaction.get("Product_ID")));
                    productStatement.setString(2, transaction.get("Product_Name"));
                    productStatement.setBigDecimal(3, new BigDecimal(transaction.get("Product_Price").replace("$", "").trim()));
                    productStatement.executeUpdate();
                }

                // Insert Stores
                try (PreparedStatement storeStatement = connection.prepareStatement(insertStoreSQL)) {
                	storeStatement.setInt(1, parseIntSafely(transaction.get("Store_ID")));
                    storeStatement.setString(2, transaction.get("Store_Name"));
                    storeStatement.executeUpdate();
                }

                // Insert Suppliers
                try (PreparedStatement supplierStatement = connection.prepareStatement(insertSupplierSQL)) {
                    supplierStatement.setInt(1, Integer.parseInt(transaction.get("Supplier_ID")));
                    supplierStatement.setString(2, transaction.get("Supplier_Name"));
                    supplierStatement.executeUpdate();
                }

                // Insert Date
                try (PreparedStatement dateStatement = connection.prepareStatement(insertDateSQL)) {
                    dateStatement.setString(1, transaction.get("Full_Date"));
                    dateStatement.setInt(2, Integer.parseInt(transaction.get("Day")));
                    dateStatement.setInt(3, Integer.parseInt(transaction.get("Month")));
                    dateStatement.setInt(4, Integer.parseInt(transaction.get("Quarter")));
                    dateStatement.setInt(5, Integer.parseInt(transaction.get("Year")));
                    dateStatement.setString(6, transaction.get("Season"));
                    dateStatement.setString(7, transaction.get("Weekday_Indicator"));
                    dateStatement.executeUpdate();
                }

                // Insert Sales
                try (PreparedStatement salesStatement = connection.prepareStatement(insertSalesSQL)) {
                    salesStatement.setInt(1, Integer.parseInt(transaction.get("Order_ID")));
                    salesStatement.setInt(2, Integer.parseInt(transaction.get("Customer_ID")));
                    salesStatement.setInt(3, Integer.parseInt(transaction.get("Product_ID")));
                    salesStatement.setInt(4, parseIntSafely(transaction.get("Store_ID")));
                    salesStatement.setInt(5, Integer.parseInt(transaction.get("Supplier_ID")));
                    salesStatement.setInt(6, Integer.parseInt(transaction.get("Quantity")));
                    salesStatement.setBigDecimal(7, new BigDecimal(transaction.get("Product_Price").replace("$", "").trim()));
                    salesStatement.setBigDecimal(8, new BigDecimal(transaction.get("SALE").replace("$", "").trim()));
                    salesStatement.executeUpdate();
                }

                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
               // e.printStackTrace();
            }
        } catch (SQLException e) {
           // e.printStackTrace();
        }
    }

}
