-- Creating sales table
CREATE TABLE sales (
Sale_ID INT,
Date date,
Store_ID INT,
Product_ID INT,
Units INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales.csv'
INTO TABLE sales
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- Inspecting and cleaning inventory table
CREATE TABLE inventory_copy LIKE inventory;
INSERT inventory_copy SELECT * FROM inventory;

SELECT * FROM inventory_copy;

SELECT *, ROW_NUMBER() OVER(
PARTITION BY Store_ID, Product_ID, Stock_On_Hand) AS row_num
FROM inventory_copy; -- assigning a number to each unique row 

WITH duplicate_cte_inventory AS
(SELECT *, ROW_NUMBER() OVER(
PARTITION BY Store_ID, Product_ID, Stock_On_Hand) AS row_num
FROM inventory_copy)
SELECT * FROM duplicate_cte_inventory WHERE row_num > 1; -- checking which rows have duplicate entries

UPDATE inventory_copy SET 
Store_ID = NULLIF(TRIM(Store_ID), ''), 
Product_ID = NULLIF(TRIM(Product_ID), ''), 
Stock_On_Hand = NULLIF(TRIM(Stock_On_Hand), '')
WHERE
TRIM(Store_ID) = '' OR
TRIM(Product_ID) = '' OR
TRIM(Stock_On_Hand) = ''; -- converting empty strings into NULL

SELECT * FROM inventory_copy
WHERE Store_ID IS NULL OR Product_ID IS NULL OR Stock_On_Hand IS NULL; -- checking NULL entries


-- Inspecting products table
CREATE TABLE products_copy LIKE products;
INSERT products_copy SELECT * FROM products;

SELECT * FROM products_copy;


-- Inspecting and cleaning sales table 
CREATE TABLE sales_copy LIKE sales;
INSERT sales_copy SELECT * FROM sales;

SELECT * FROM sales_copy;

WITH duplicate_cte_sales AS 
(SELECT *, ROW_NUMBER() OVER(
PARTITION BY Sale_ID, Date, Store_ID, Product_ID, Units) AS row_num
FROM sales_copy)
SELECT * from duplicate_cte_sales WHERE row_num > 1; -- checking for duplicate rows

SELECT Sale_ID, COUNT(*) AS occurrences FROM sales_copy GROUP BY Sale_ID HAVING COUNT(*) > 1; -- checking for duplicate Sale_IDs

UPDATE sales_copy SET 
Sale_ID = NULLIF(TRIM(Sale_ID), ''), 
Date = NULLIF(TRIM(Date), ''), 
Store_ID = NULLIF(TRIM(Store_ID), ''),
Product_ID = NULLIF(TRIM(Product_ID), ''),
Units = NULLIF(TRIM(Units), '')
WHERE
TRIM(Sale_ID) = '' OR
TRIM(Date) = '' OR
TRIM(Store_ID) = '' OR
TRIM(Product_ID) = '' OR
TRIM(Units) = ''; -- converting empty strings into NULL

SELECT * FROM sales_copy
WHERE Sale_ID IS NULL OR Date IS NULL OR Store_ID IS NULL OR Product_ID IS NULL OR Units IS NULL; -- checking NULL entries


-- Inspecting stores table 
CREATE TABLE stores_copy LIKE stores;
INSERT stores_copy SELECT * FROM stores;

SELECT * FROM stores_copy;

