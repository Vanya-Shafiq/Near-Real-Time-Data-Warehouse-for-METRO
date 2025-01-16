package metro;

import java.io.*;
import java.util.*;

public class MasterDataLoader {

    public static List<Map<String, String>> loadProductData() {
        List<Map<String, String>> productData = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\786\\Downloads\\products_data.csv"))) {
            String line;
            // Skip header
            br.readLine();
            
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                Map<String, String> product = new HashMap<>();
                product.put("Product_ID", values[0]);
                product.put("Product_Name", values[1]);
                product.put("Product_Price", values[2]);
                product.put("Supplier_ID", values[3]);
                product.put("Supplier_Name", values[4]);
                product.put("Store_ID", values[5]);
                product.put("Store_Name", values[6]);
                productData.add(product);
            }
        } catch (IOException e) {
            System.err.println("Product Data Loader Error: " + e.getMessage());
        }
        return productData;
    }

    public static List<Map<String, String>> loadCustomerData() {
        List<Map<String, String>> customerData = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\786\\Downloads\\customers_data.csv"))) {
            String line;
            // Skip header
            br.readLine();
            
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                Map<String, String> customer = new HashMap<>();
                customer.put("Customer_ID", values[0]);
                customer.put("Customer_Name", values[1]);
                customer.put("Gender", values[2]);
                customerData.add(customer);
            }
        } catch (IOException e) {
            System.err.println("Customer Data Loader Error: " + e.getMessage());
        }
        return customerData;
    }
}