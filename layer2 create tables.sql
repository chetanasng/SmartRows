/*
===============================================================================
DDL Script: Create Layer2 Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'layer2' schema, dropping existing tables 
    if they already exist.
    Run this script to re-define the DDL structure of 'layer1' Tables
===============================================================================
*/
drop schema if exists  layer2;
create schema layer2;
-- Drop and create the customer information table.
DROP TABLE IF EXISTS layer2.crm_cust_info;

CREATE TABLE layer2.crm_cust_info (
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create the product information table.
DROP TABLE IF EXISTS layer2.crm_prd_info;

CREATE TABLE layer2.crm_prd_info (
    prd_id          INT,
    cat_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(50),
    prd_cost        INT,
    prd_line        VARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create the sales details table.
DROP TABLE IF EXISTS layer2.crm_sales_details;

CREATE TABLE layer2.crm_sales_details (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create the ERP location table.
DROP TABLE IF EXISTS layer2.erp_loc_a101;

CREATE TABLE layer2.erp_loc_a101 (
    cid             VARCHAR(50),
    cntry           VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create the ERP customer table.
DROP TABLE IF EXISTS layer2.erp_cust_az12;

CREATE TABLE layer2.erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create the ERP product category table.
DROP TABLE IF EXISTS layer2.erp_px_cat_g1v2;

CREATE TABLE layer2.erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- Checks done: 

-- check for nulls and duplicates in primary key, expecting nulls

select cst_id, count(*)
from layer1.crm_cust_info
group by cst_id
having count(*) > 1;



