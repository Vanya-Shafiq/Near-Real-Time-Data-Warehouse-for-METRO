DROP DATABASE IF EXISTS metro;
CREATE DATABASE metro;
USE metro;

-- Drop existing schema if it exists
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS stores;
DROP TABLE IF EXISTS date;
DROP TABLE IF EXISTS suppliers;

-- Create Dimension Tables
CREATE TABLE customers (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(100),
    Gender VARCHAR(20) 
);

CREATE TABLE products (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(100),
    Product_Price DECIMAL(10, 2)
);

CREATE TABLE stores (
    Store_ID INT PRIMARY KEY,
    Store_Name VARCHAR(100)
);

CREATE TABLE suppliers (
    Supplier_ID INT PRIMARY KEY,
    Supplier_Name VARCHAR(100)
);

CREATE TABLE date (
    Date_ID INT PRIMARY KEY AUTO_INCREMENT,
    Full_Date DATE,
    Day INT,
    Month INT,
    Quarter INT,
    Year INT,
    Season VARCHAR(100),
    Weekday_Indicator VARCHAR(10) -- Weekday(W)/Weekend(E)
);

-- Create Fact Table
CREATE TABLE sales (
    Transaction_ID INT PRIMARY KEY,
    Customer_ID INT,
    Product_ID INT,
    Store_ID INT,
    Date_ID INT AUTO_INCREMENT,
    Supplier_ID INT,
    Quantity INT,
    Price DECIMAL(10, 2),
    Total_Sale DECIMAL(10, 2),
    FOREIGN KEY (Customer_ID) REFERENCES customers(Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES products(Product_ID),
    FOREIGN KEY (Store_ID) REFERENCES stores(Store_ID),
    FOREIGN KEY (Date_ID) REFERENCES date(Date_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES suppliers(Supplier_ID)
);
ALTER TABLE sales MODIFY COLUMN Customer_ID INT NULL;


