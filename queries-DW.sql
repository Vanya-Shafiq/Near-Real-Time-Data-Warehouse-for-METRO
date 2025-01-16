-- OLAP Queries
-- 1
SELECT 
    p.Product_Name,
    d.Month,
    d.Weekday_Indicator,
    SUM(s.Total_Sale) AS Total_Revenue
FROM 
    sales s
JOIN 
    products p ON s.Product_ID = p.Product_ID
JOIN 
    date d ON s.Date_ID = d.Date_ID
WHERE 
    d.Year = 2024 -- Specify desired year
GROUP BY 
    p.Product_Name, d.Month, d.Weekday_Indicator
ORDER BY 
    d.Month, d.Weekday_Indicator, Total_Revenue DESC
LIMIT 5;

-- 2
-- Calculate total revenue for each store by quarter in 2017
WITH QuarterlyRevenue AS (
    SELECT 
        s.Store_ID,
        d.Year,
        d.Quarter,
        SUM(s.Total_Sale) AS Total_Revenue
    FROM 
        sales s
    JOIN 
        date d ON s.Date_ID = d.Date_ID
    WHERE 
        d.Year = 2017
    GROUP BY 
        s.Store_ID, d.Year, d.Quarter
),

-- Calculate revenue growth rate
GrowthRate AS (
    SELECT 
        q1.Store_ID,
        q1.Quarter AS Current_Quarter,
        q1.Total_Revenue AS Current_Revenue,
        q2.Total_Revenue AS Previous_Revenue,
        CASE 
            WHEN q2.Total_Revenue IS NULL THEN NULL
            ELSE ((q1.Total_Revenue - q2.Total_Revenue) / q2.Total_Revenue) * 100
        END AS Growth_Rate
    FROM 
        QuarterlyRevenue q1
    LEFT JOIN 
        QuarterlyRevenue q2 
        ON q1.Store_ID = q2.Store_ID 
        AND q1.Quarter = q2.Quarter + 1
)

-- Display the results
SELECT 
    Store_ID,
    Current_Quarter,
    Current_Revenue,
    Previous_Revenue,
    Growth_Rate
FROM 
    GrowthRate
ORDER BY 
    Store_ID, Current_Quarter;


-- 3
SELECT 
    st.Store_Name,
    sp.Supplier_Name,
    p.Product_Name,
    SUM(s.Total_Sale) AS Supplier_Contribution
FROM 
    sales s
JOIN 
    stores st ON s.Store_ID = st.Store_ID
JOIN 
    suppliers sp ON s.Supplier_ID = sp.Supplier_ID
JOIN 
    products p ON s.Product_ID = p.Product_ID
GROUP BY 
    st.Store_Name, sp.Supplier_Name, p.Product_Name
ORDER BY 
    st.Store_Name, sp.Supplier_Name, Supplier_Contribution DESC;


-- 4
SELECT 
    p.Product_Name,
    d.Season,
    SUM(s.Total_Sale) AS Total_Sales
FROM 
    sales s
JOIN 
    products p ON s.Product_ID = p.Product_ID
JOIN 
    date d ON s.Date_ID = d.Date_ID
GROUP BY 
    p.Product_Name, d.Season
ORDER BY 
    d.Season, Total_Sales DESC;

-- 5
WITH MonthlyRevenue AS (
    -- Calculate the total revenue for each store and supplier by month
    SELECT
        s.Store_ID,
        s.Supplier_ID,
        d.Year,
        d.Month,
        SUM(sa.Total_Sale) AS Monthly_Revenue
    FROM
        sales sa
    JOIN date d ON sa.Date_ID = d.Date_ID
    JOIN stores s ON sa.Store_ID = s.Store_ID
    GROUP BY
        s.Store_ID, s.Supplier_ID, d.Year, d.Month
),
RevenueChange AS (
    -- Calculate the percentage change in revenue from one month to the next for each store and supplier pair
    SELECT
        a.Store_ID,
        a.Supplier_ID,
        a.Year,
        a.Month,
        a.Monthly_Revenue,
        ((a.Monthly_Revenue - b.Monthly_Revenue) / b.Monthly_Revenue) * 100 AS Revenue_Volatility
    FROM
        MonthlyRevenue a
    LEFT JOIN MonthlyRevenue b
        ON a.Store_ID = b.Store_ID
        AND a.Supplier_ID = b.Supplier_ID
        AND a.Year = b.Year
        AND a.Month = b.Month + 1 -- Compare with the previous month
)
-- Select the final results
SELECT
    Store_ID,
    Supplier_ID,
    Year,
    Month,
    Revenue_Volatility
FROM
    RevenueChange
ORDER BY
    Store_ID, Supplier_ID, Year, Month;


-- 6
SELECT 
    p1.Product_Name AS Product1,
    p2.Product_Name AS Product2,
    COUNT(*) AS Frequency
FROM 
    sales s1
JOIN 
    sales s2 ON s1.Transaction_ID = s2.Transaction_ID AND s1.Product_ID < s2.Product_ID
JOIN 
    products p1 ON s1.Product_ID = p1.Product_ID
JOIN 
    products p2 ON s2.Product_ID = p2.Product_ID
GROUP BY 
    p1.Product_Name, p2.Product_Name
ORDER BY 
    Frequency DESC
LIMIT 5;

-- 7
SELECT 
    st.Store_Name,
    sp.Supplier_Name,
    p.Product_Name,
    SUM(s.Total_Sale) AS Total_Revenue
FROM 
    sales s
JOIN 
    stores st ON s.Store_ID = st.Store_ID
JOIN 
    suppliers sp ON s.Supplier_ID = sp.Supplier_ID
JOIN 
    products p ON s.Product_ID = p.Product_ID
GROUP BY 
    ROLLUP(st.Store_Name, sp.Supplier_Name, p.Product_Name)
ORDER BY 
    st.Store_Name, sp.Supplier_Name, p.Product_Name;



-- 8
SELECT 
    p.Product_Name,
    CASE 
        WHEN d.Month BETWEEN 1 AND 6 THEN 'H1'
        ELSE 'H2'
    END AS Half_Year,
    SUM(s.Total_Sale) AS Total_Revenue,
    SUM(s.Quantity) AS Total_Quantity
FROM 
    sales s
JOIN 
    products p ON s.Product_ID = p.Product_ID
JOIN 
    date d ON s.Date_ID = d.Date_ID
GROUP BY 
    p.Product_Name, Half_Year
ORDER BY 
    p.Product_Name, Half_Year;

-- 9
WITH ProductDailySales AS (
    SELECT 
        p.Product_Name,
        d.Full_Date,
        AVG(s.Total_Sale) OVER (PARTITION BY p.Product_Name) AS Daily_Avg,
        SUM(s.Total_Sale) AS Daily_Total
    FROM 
        sales s
    JOIN 
        products p ON s.Product_ID = p.Product_ID
    JOIN 
        date d ON s.Date_ID = d.Date_ID
    GROUP BY 
        p.Product_Name, d.Full_Date
)
SELECT 
    Product_Name,
    Full_Date,
    Daily_Total,
    Daily_Avg,
    CASE 
        WHEN Daily_Total > 2 * Daily_Avg THEN 'Outlier'
        ELSE 'Normal'
    END AS Spike_Flag
FROM 
    ProductDailySales;

-- 10
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    st.Store_Name,
    d.Quarter,
    SUM(s.Total_Sale) AS Total_Quarterly_Sales
FROM 
    sales s
JOIN 
    stores st ON s.Store_ID = st.Store_ID
JOIN 
    date d ON s.Date_ID = d.Date_ID
GROUP BY 
    st.Store_Name, d.Quarter
ORDER BY 
    st.Store_Name, d.Quarter;
