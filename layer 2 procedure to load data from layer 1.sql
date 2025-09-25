/*
===============================================================================
Stored Procedure: load_layer2_from_layer1
===============================================================================
Purpose:
    This procedure handles the ETL (Extract, Transform, Load) process to
    populate the cleaned 'layer2' tables from the raw 'layer1' tables.
    It includes data cleansing, standardization, and deduplication.
===============================================================================
*/

-- Drop the procedure if it already exists to allow for re-creation.
DROP PROCEDURE IF EXISTS layer2.load_layer2_from_layer1;

-- Change the statement delimiter to $$ to allow semicolons inside the procedure.
DELIMITER $$

CREATE PROCEDURE layer2.load_layer2_from_layer1()
BEGIN
    /*
    ---------------------------------------------------------------------------
    Table: crm_cust_info
    Transformations:
    1. Deduplication: Keeps only the most recent record for each customer ID.
    2. Standardization: Converts single-letter codes for marital status and
       gender into full, readable text (e.g., 'S' -> 'Single').
    3. Cleansing: Removes leading/trailing whitespace from names.
    ---------------------------------------------------------------------------
    */
    TRUNCATE TABLE layer2.crm_cust_info;
    INSERT INTO layer2.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        -- This subquery uses a window function to find the latest record for each customer.
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM layer1.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) AS t
    WHERE flag_last = 1; -- The final select keeps only the most recent record (flag_last = 1).


    /*
    ---------------------------------------------------------------------------
    Table: crm_prd_info
    Transformations:
    1. Feature Engineering: Extracts a 'cat_id' from the product key.
    2. Data Cleansing: Cleans the product key by removing the category prefix.
    3. Standardization: Maps product line codes to descriptive names (e.g., 'M' -> 'Mountain').
    4. Handling Missing Values: Replaces NULL product costs with 0.
    5. Slowly Changing Dimension (SCD Type 2): Calculates the prd_end_dt for
       historical tracking, making the old record expire the day before the new one starts.
    ---------------------------------------------------------------------------
    */
    TRUNCATE TABLE layer2.crm_prd_info;
    INSERT INTO layer2.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost,
        prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        IFNULL(prd_cost, 0) AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        -- LEAD function looks at the next row's start_dt to calculate the current row's end_dt.
        CAST(
            DATE_SUB(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY)
        AS DATE) AS prd_end_dt
    FROM layer1.crm_prd_info;


    /*
    ---------------------------------------------------------------------------
    Table: crm_sales_details
    Transformations:
    1. Data Type Conversion: Converts integer dates (YYYYMMDD) into a proper DATE format.
    2. Data Validation & Correction: Recalculates the 'sls_sales' total if it's missing,
       zero, or inconsistent with quantity and price.
    3. Data Imputation: Derives 'sls_price' from sales and quantity if the price is missing.
    ---------------------------------------------------------------------------
    */
    TRUNCATE TABLE layer2.crm_sales_details;
    INSERT INTO layer2.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
        sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Safely convert integer to date, handling invalid formats.
        CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d') END,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d') END,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d') END,
        -- If sales amount is invalid, recalculate it. This ensures data integrity.
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        -- If price is invalid, derive it from sales and quantity.
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM layer1.crm_sales_details;


    /*
    ---------------------------------------------------------------------------
    Table: erp_loc_a101
    Transformations:
    1. Data Cleansing: Removes hyphens from customer IDs for consistency.
    2. Standardization: Maps country codes to full country names (e.g., 'US' -> 'United States').
    3. Handling Missing Values: Replaces blank or NULL country info with 'n/a'.
    ---------------------------------------------------------------------------
    */
    TRUNCATE TABLE layer2.erp_loc_a101;
    INSERT INTO layer2.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM layer1.erp_loc_a101;


    /*
    ---------------------------------------------------------------------------
    Table: erp_cust_az12
    Transformations:
    1. Data Cleansing: Removes the 'NAS' prefix from customer IDs.
    2. Data Validation: Invalidates future birth dates by setting them to NULL.
    3. Standardization: Normalizes various gender inputs ('F', 'FEMALE') to a consistent format.
    ---------------------------------------------------------------------------
    */
    TRUNCATE TABLE layer2.erp_cust_az12;
    INSERT INTO layer2.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) ELSE cid END AS cid,
        CASE WHEN bdate > CURDATE() THEN NULL ELSE bdate END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM layer1.erp_cust_az12;


    /*
    ---------------------------------------------------------------------------
    Table: erp_px_cat_g1v2
    Transformations:
    1. Passthrough: This is a simple load with no transformations, moving data
       as-is from the source layer. This is common for lookup or reference data.
    ---------------------------------------------------------------------------
    */
    TRUNCATE TABLE layer2.erp_px_cat_g1v2;
    INSERT INTO layer2.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance FROM layer1.erp_px_cat_g1v2;

END$$

-- Reset the delimiter back to the standard semicolon.
DELIMITER ;


