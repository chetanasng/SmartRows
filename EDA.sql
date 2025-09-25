/*
================================================================================
-- Data Warehouse Analysis Script (MySQL Version)
--
-- Description:
-- This is a consolidated script containing a series of analytical queries
-- against the data warehouse, converted to MySQL format.
--
-- Schema Used: layer3 (Gold Layer)
================================================================================
*/


/*
===============================================================================
-- Section 01: Database Exploration
-- Purpose: To explore the structure of the database and inspect table metadata.
===============================================================================
*/

-- Retrieve a list of all tables in the database
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES;

-- Retrieve all columns for a specific table (dim_customers)
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers' AND TABLE_SCHEMA = 'layer3';


/*
===============================================================================
-- Section 02: Dimensions Exploration
-- Purpose: To explore the structure and unique values within dimension tables.
===============================================================================
*/

-- Retrieve a list of unique countries from which customers originate
SELECT DISTINCT
    country
FROM layer3.dim_customers
ORDER BY country;

-- Retrieve a list of unique categories, subcategories, and products
SELECT DISTINCT
    category,
    subcategory,
    product_name
FROM layer3.dim_products
ORDER BY category, subcategory, product_name;


/*
===============================================================================
-- Section 03: Date Range Exploration
-- Purpose: To determine the temporal boundaries of the data.
===============================================================================
*/

-- Determine the first and last order date and the total duration in months
SELECT
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    -- MySQL's PERIOD_DIFF is used to calculate the difference in months
    PERIOD_DIFF(
        DATE_FORMAT(MAX(order_date), '%Y%m'),
        DATE_FORMAT(MIN(order_date), '%Y%m')
    ) AS order_range_months
FROM layer3.fact_sales;

-- Find the youngest and oldest customer based on birthdate
SELECT
    MIN(birthdate) AS oldest_birthdate,
    -- MySQL uses TIMESTAMPDIFF to get the age in years
    TIMESTAMPDIFF(YEAR, MIN(birthdate), CURDATE()) AS oldest_age,
    MAX(birthdate) AS youngest_birthdate,
    TIMESTAMPDIFF(YEAR, MAX(birthdate), CURDATE()) AS youngest_age
FROM layer3.dim_customers;


/*
===============================================================================
-- Section 04: Measures Exploration (Key Metrics)
-- Purpose: To calculate aggregated metrics for quick business insights.
===============================================================================
*/

-- Find the Total Sales
SELECT SUM(sales_amount) AS total_sales FROM layer3.fact_sales;

-- Find how many items are sold
SELECT SUM(quantity) AS total_quantity FROM layer3.fact_sales;

-- Find the average selling price
SELECT AVG(price) AS avg_price FROM layer3.fact_sales;

-- Find the Total number of Orders
SELECT COUNT(DISTINCT order_number) AS total_orders FROM layer3.fact_sales;

-- Find the total number of products
SELECT COUNT(product_name) AS total_products FROM layer3.dim_products;

-- Find the total number of customers
SELECT COUNT(customer_key) AS total_customers FROM layer3.dim_customers;

-- Find the total number of customers that have placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM layer3.fact_sales;

-- Generate a Report that shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM layer3.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM layer3.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM layer3.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM layer3.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_name) FROM layer3.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM layer3.dim_customers;


/*
===============================================================================
-- Section 05: Magnitude Analysis
-- Purpose: To quantify data and group results by specific dimensions.
===============================================================================
*/

-- Find total customers by countries
SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM layer3.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Find total customers by gender
SELECT
    gender,
    COUNT(customer_key) AS total_customers
FROM layer3.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- Find total products by category
SELECT
    category,
    COUNT(product_key) AS total_products
FROM layer3.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- What is the average cost in each category?
SELECT
    category,
    AVG(cost) AS avg_cost
FROM layer3.dim_products
GROUP BY category
ORDER BY avg_cost DESC;

-- What is the total revenue generated for each category?
SELECT
    p.category,
    SUM(f.sales_amount) AS total_revenue
FROM layer3.fact_sales f
LEFT JOIN layer3.dim_products p ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- What is the total revenue generated by each customer?
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM layer3.fact_sales f
LEFT JOIN layer3.dim_customers c ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC;

-- What is the distribution of sold items across countries?
SELECT
    c.country,
    SUM(f.quantity) AS total_quantity_sold
FROM layer3.fact_sales f
LEFT JOIN layer3.dim_customers c ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_quantity_sold DESC;


/*
===============================================================================
-- Section 06: Ranking Analysis
-- Purpose: To rank items based on performance to identify top performers.
===============================================================================
*/

-- Which 5 products Generating the Highest Revenue? (Simple Ranking with LIMIT)
SELECT
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM layer3.fact_sales f
LEFT JOIN layer3.dim_products p ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5;

-- Flexible Ranking Using Window Functions
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
    FROM layer3.fact_sales f
    LEFT JOIN layer3.dim_products p ON p.product_key = f.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE rank_products <= 5;

-- What are the 5 worst-performing products in terms of sales?
SELECT
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM layer3.fact_sales f
LEFT JOIN layer3.dim_products p ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue ASC
LIMIT 5;

-- Find the top 10 customers who have generated the highest revenue
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM layer3.fact_sales f
LEFT JOIN layer3.dim_customers c ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC
LIMIT 10;


/*
===============================================================================
-- Section 07: Change Over Time Analysis
-- Purpose: To track trends, growth, and seasonality in key metrics.
===============================================================================
*/

-- Analyse sales performance over time by year and month
SELECT
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM layer3.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

-- Using DATE_FORMAT to truncate to the month
SELECT
    DATE_FORMAT(order_date, '%Y-%m-01') AS order_month_start,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM layer3.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_month_start
ORDER BY order_month_start;


/*
===============================================================================
-- Section 08: Cumulative Analysis
-- Purpose: To calculate running totals to track cumulative growth.
===============================================================================
*/

-- Calculate the total sales per year and the running total of sales over time
SELECT
    order_year,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_year) AS running_total_sales,
    AVG(avg_price) OVER (ORDER BY order_year) AS moving_average_price
FROM (
    SELECT
        YEAR(order_date) AS order_year,
        SUM(sales_amount) AS total_sales,
        AVG(price) AS avg_price
    FROM layer3.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY order_year
) t;


/*
===============================================================================
-- Section 09: Performance Analysis (Year-over-Year)
-- Purpose: To measure and benchmark performance over time.
===============================================================================
*/

-- Analyze the yearly performance of products vs. average and previous year
WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM layer3.fact_sales f
    LEFT JOIN layer3.dim_products p ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_from_avg,
    LAG(current_sales, 1, 0) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
    current_sales - LAG(current_sales, 1, 0) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_from_previous_year
FROM yearly_product_sales
ORDER BY product_name, order_year;


/*
===============================================================================
-- Section 10: Data Segmentation Analysis
-- Purpose: To group data into meaningful categories for targeted insights.
===============================================================================
*/

-- Segment products into cost ranges and count products in each segment
WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM layer3.dim_products
)
SELECT
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;

-- Group customers into segments based on their spending behavior and lifespan
WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(f.sales_amount) AS total_spending,
        PERIOD_DIFF(
            DATE_FORMAT(MAX(order_date), '%Y%m'),
            DATE_FORMAT(MIN(order_date), '%Y%m')
        ) AS lifespan_months
    FROM layer3.fact_sales f
    LEFT JOIN layer3.dim_customers c ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM (
    SELECT
        customer_key,
        CASE
            WHEN lifespan_months < 12 THEN 'New'
            WHEN lifespan_months >= 12 AND total_spending > 5000 THEN 'VIP'
            ELSE 'Regular'
        END AS customer_segment
    FROM customer_spending
) segmented_customers
GROUP BY customer_segment;


/*
===============================================================================
-- Section 11: Part-to-Whole Analysis
-- Purpose: To evaluate the contribution of different categories to the total.
===============================================================================
*/
-- Which categories contribute the most to overall sales?
WITH category_sales AS (
    SELECT
        p.category,
        SUM(f.sales_amount) AS total_sales
    FROM layer3.fact_sales f
    LEFT JOIN layer3.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.category
)
SELECT
    category,
    total_sales,
    SUM(total_sales) OVER () AS overall_sales,
    ROUND((total_sales / SUM(total_sales) OVER ()) * 100, 2) AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;


/*
===============================================================================
-- Section 12: Customer Report View
-- Purpose: To create a consolidated view for comprehensive customer analysis.
===============================================================================
*/

DROP VIEW IF EXISTS layer3.report_customers;

CREATE VIEW layer3.report_customers AS
WITH base_query AS (
    -- Step 1: Retrieves core columns from tables
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        TIMESTAMPDIFF(YEAR, c.birthdate, CURDATE()) AS age
    FROM layer3.fact_sales f
    LEFT JOIN layer3.dim_customers c ON c.customer_key = f.customer_key
    WHERE f.order_date IS NOT NULL
),
customer_aggregation AS (
    -- Step 2: Summarizes key metrics at the customer level
    SELECT
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        PERIOD_DIFF(DATE_FORMAT(MAX(order_date), '%Y%m'), DATE_FORMAT(MIN(order_date), '%Y%m')) AS lifespan
    FROM base_query
    GROUP BY
        customer_key,
        customer_number,
        customer_name,
        age
)
-- Step 3: Final report with calculated KPIs
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50 and above'
    END AS age_group,
    CASE
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    last_order_date,
    PERIOD_DIFF(DATE_FORMAT(CURDATE(), '%Y%m'), DATE_FORMAT(last_order_date, '%Y%m')) AS recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
    -- Compute average order value (AOV)
    CASE WHEN total_orders = 0 THEN 0
         ELSE total_sales / total_orders
    END AS avg_order_value,
    -- Compute average monthly spend
    CASE WHEN lifespan = 0 THEN total_sales
         ELSE total_sales / (lifespan + 1) -- Add 1 to avoid division by zero for new customers
    END AS avg_monthly_spend
FROM customer_aggregation;


/*
===============================================================================
-- Section 13: Product Report View
-- Purpose: To create a consolidated view for comprehensive product analysis.
===============================================================================
*/

DROP VIEW IF EXISTS layer3.report_products;

CREATE VIEW layer3.report_products AS
WITH base_query AS (
    -- Step 1: Retrieves core columns from fact_sales and dim_products
    SELECT
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM layer3.fact_sales f
    LEFT JOIN layer3.dim_products p ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
),
product_aggregations AS (
    -- Step 2: Summarizes key metrics at the product level
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        PERIOD_DIFF(DATE_FORMAT(MAX(order_date), '%Y%m'), DATE_FORMAT(MIN(order_date), '%Y%m')) AS lifespan,
        MAX(order_date) AS last_sale_date,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        ROUND(AVG(sales_amount / NULLIF(quantity, 0)), 2) AS avg_selling_price
    FROM base_query
    GROUP BY
        product_key,
        product_name,
        category,
        subcategory,
        cost
)
-- Step 3: Final report with calculated KPIs
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    PERIOD_DIFF(DATE_FORMAT(CURDATE(), '%Y%m'), DATE_FORMAT(last_sale_date, '%Y%m')) AS recency_in_months,
    CASE
        WHEN total_sales > 50000 THEN 'High-Performer'
        WHEN total_sales >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    -- Average Order Revenue (AOR)
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_revenue,
    -- Average Monthly Revenue
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / (lifespan + 1) -- Add 1 to avoid division by zero
    END AS avg_monthly_revenue
FROM product_aggregations;
