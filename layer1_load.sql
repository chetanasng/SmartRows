create schema layer1;


-- Drop and create the customer information table from the CRM system.
DROP TABLE IF EXISTS layer1.crm_cust_info;

CREATE TABLE layer1.crm_cust_info (
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE
);

-- Drop and create the product information table from the CRM system.
DROP TABLE IF EXISTS layer1.crm_prd_info;

CREATE TABLE layer1.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

-- Drop and create the sales details table from the CRM system.
DROP TABLE IF EXISTS layer1.crm_sales_details;

CREATE TABLE layer1.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-- Drop and create the location table from the ERP system (loc_a101).
DROP TABLE IF EXISTS layer1.erp_loc_a101;

CREATE TABLE layer1.erp_loc_a101 (
    cid   VARCHAR(50),
    cntry VARCHAR(50)
);

-- Drop and create the customer table from the ERP system (cust_az12).
DROP TABLE IF EXISTS layer1.erp_cust_az12;

CREATE TABLE layer1.erp_cust_az12 (
    cid   VARCHAR(50),
    bdate DATE,
    gen   VARCHAR(50)
);

-- Drop and create the product category table from the ERP system (px_cat_g1v2).
DROP TABLE IF EXISTS layer1.erp_px_cat_g1v2;

CREATE TABLE layer1.erp_px_cat_g1v2 (
    id          VARCHAR(50),
    cat         VARCHAR(50),
    subcat      VARCHAR(50),
    maintenance VARCHAR(50)
);


-- Load datasets from csv file using Table Data Import Wizard

select * from crm_cust_info limit 5;
select count(*) from crm_prd_info ;
select count(*) from crm_sales_details ;

select count(*) from erp_cust_az12;
select count(*) from erp_loc_a101;
select count(*) from erp_px_cat_g1v2;

