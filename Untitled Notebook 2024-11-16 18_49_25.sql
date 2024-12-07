-- Databricks notebook source
select * from customers_data

-- COMMAND ----------

CREATE TABLE delta_sales_by_product
USING DELTA
SELECT 
    p.ProductID,
    p.Category,
    p.Brand,
    SUM(s.Quantity * s.Price) AS Total_Sales,
    COUNT(s.OrderID) AS Total_Transactions
FROM 
    hive_metastore.default.sales_data s
JOIN 
    hive_metastore.default.products_data p
ON 
    s.ProductID = p.ProductID
GROUP BY 
    p.ProductID, p.Category, p.Brand
ORDER BY 
    Total_Sales DESC;

-- COMMAND ----------

select * from delta_sales_by_product

-- COMMAND ----------

CREATE TABLE delta_sales_by_region
USING DELTA
SELECT 
    c.Region,
    COUNT(DISTINCT s.OrderID) AS Total_Orders,
    SUM(s.Quantity) AS Total_Quantity_Sold,
    ROUND(SUM(s.Quantity * s.Price), 2) AS Total_Revenue
FROM 
    merged_sales_customer_data s -- Adjusted to the correct table name
JOIN 
    hive_metastore.default.customers_data c ON s.CustomerID = c.CustomerID
GROUP BY 
    c.Region
ORDER BY 
    Total_Revenue DESC;

-- COMMAND ----------

select * from delta_sales_by_region

-- COMMAND ----------

CREATE TABLE delta_sales_by_store
USING DELTA
SELECT 
    s.Store,
    COUNT(DISTINCT s.OrderID) AS Total_Orders,
    SUM(s.Quantity) AS Total_Quantity_Sold,
    ROUND(SUM(s.Quantity * s.Price), 2) AS Total_Revenue
FROM 
    merged_sales_customer_data s
GROUP BY 
    s.Store
ORDER BY 
    Total_Revenue DESC;

-- COMMAND ----------

select * from delta_sales_by_store

-- COMMAND ----------

CREATE TABLE delta_sales_by_category
USING DELTA
SELECT 
    p.Category,
    SUM(s.Quantity * s.Price) AS Total_Sales,
    COUNT(DISTINCT s.OrderID) AS Total_Transactions
FROM 
    merged_sales_customer_data s
JOIN 
    products_data p
ON 
    s.ProductID = p.ProductID
GROUP BY 
    p.Category
ORDER BY 
    Total_Sales DESC;

-- COMMAND ----------

select * from delta_sales_by_category

-- COMMAND ----------

CREATE TABLE delta_frequency_of_sales_and_total_spending
USING DELTA
SELECT 
    c.CustomerID,
    c.Region,
    COUNT(s.OrderID) AS Total_Orders,
    SUM(s.Quantity * s.Price) AS Total_Spending,
    AVG(s.Quantity * s.Price) AS Avg_Order_Value
FROM 
    merged_sales_customer_data s
JOIN 
    customers_data c
ON 
    s.CustomerID = c.CustomerID
GROUP BY 
    c.CustomerID, c.Region
ORDER BY 
    Total_Spending DESC;

-- COMMAND ----------

select * from delta_frequency_of_sales_and_total_spending

-- COMMAND ----------

CREATE TABLE delta_monthly_sales_trends
USING DELTA
SELECT 
    DATE_TRUNC('month', s.Date) AS Month,
    SUM(s.Quantity * s.Price) AS Total_Sales,
    COUNT(DISTINCT s.OrderID) AS Total_Orders
FROM 
    merged_sales_customer_data s
GROUP BY 
    DATE_TRUNC('month', s.Date)
ORDER BY 
    Month;

-- COMMAND ----------

select * from delta_monthly_sales_trends

-- COMMAND ----------

CREATE TABLE delta_weekly_sales_trends
USING DELTA
SELECT 
    DATE_TRUNC('week', s.Date) AS Week,
    SUM(s.Quantity * s.Price) AS Total_Sales,
    COUNT(DISTINCT s.OrderID) AS Total_Orders
FROM 
    merged_sales_customer_data s
GROUP BY 
    DATE_TRUNC('week', s.Date)
ORDER BY 
    Week;

-- COMMAND ----------

select * from delta_weekly_sales_trends

-- COMMAND ----------

CREATE OR REPLACE VIEW default.sales_overview_by_region AS
SELECT 
    c.Region AS Region,
    COUNT(DISTINCT s.OrderID) AS Total_Orders,
    SUM(s.Quantity) AS Total_Quantity_Sold,
    ROUND(SUM(s.Quantity * s.Price), 2) AS Total_Revenue,
    COUNT(DISTINCT c.CustomerID) AS Total_Customers
FROM 
    sales_data s
JOIN 
    customers_data c 
ON 
    s.Store = c.CustomerID
GROUP BY 
    c.Region
ORDER BY 
    Total_Revenue DESC;

-- COMMAND ----------



-- COMMAND ----------

