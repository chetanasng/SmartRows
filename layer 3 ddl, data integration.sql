/* in layer 3 we explore and understand the business objects
focus on data integration, create data objects that we have identified
dimension or fact
rename 
validate 
document 
data model
data dictionary 
data flow diagram

data modelling - unorganised data to organising and structuring data in meaningful way by putting it in objects each focused 
on specific info and describe relationship between the objects 
ways: 
	1. BIG PICTURE: conceptual data model (dont go in details but focus on entities and gives the big picture)
    2. BLUEPRINT: logical data model (specify diff cols we find in each entity and draw relationship
		specify PK, not worry ab storage)
	3. IMPLEMENTATION: physical dm (etchnical details, datatype, length etc) - databricks automatically generates

star schema, central fact table (measurements, transactions) and surrounded by dimensions (descriptive info)
snowflake schema - fact table in middle, surrounded by dim but dim broken into smaller sub dimensions

star schema easier to understand and query
but dimension may contain duplicates and may grow larger w time

snowflake complex but good for larger datasets and removed redundancies harder to query
*/


/*
dimension has descriptive info that give context to data WHO WHAT WHERE
fact atble - dates, measures, numbers, transactions and represents events , quantitative HOW MUCH? HOW MANY
*/


/*
here we break the oriented system to sources and create completely new for business
detect business objects in the source systems (hidden)
*/

/*
===============================================================================
DDL Script: Create Layer3 Views
===============================================================================
Script Purpose:
    This script creates the final presentation layer (views) for analytics.
    It builds a Star Schema consisting of Dimension and Fact tables by joining
    and transforming data from the 'layer2' schema.

    These views are the clean, business-ready source for reports and dashboards.
===============================================================================
*/

-- =============================================================================
-- Create Dimension View: layer3.dim_customers
-- Objective: To create a single, comprehensive table containing all descriptive
--            attributes about a customer. This is a classic dimension table.
-- =============================================================================

drop schema if exists layer3;
create schema layer3;

DROP VIEW IF EXISTS layer3.dim_customers;

CREATE VIEW layer3.dim_customers AS
SELECT
    -- A surrogate key is a unique, system-generated key. It is best practice
    -- because it's stable and protects the warehouse from changes in source system keys.
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,

    -- Business keys are the original IDs from the source systems.
    -- They are kept for traceability back to the source.
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,

    -- Descriptive attributes are consolidated from multiple source tables.
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,

    -- Data Enrichment: This logic combines the best available data from two
    -- different source systems (CRM and ERP) into a single, reliable field.
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source
        ELSE COALESCE(ca.gen, 'n/a')              -- ERP is the fallback
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM
    layer2.crm_cust_info ci
    -- Joining different customer data sources on the common business key.
    LEFT JOIN layer2.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT JOIN layer2.erp_loc_a101 la ON ci.cst_key = la.cid;


-- =============================================================================
-- Create Dimension View: layer3.dim_products
-- Objective: To create a master product table with all relevant details,
--            including category information.
-- =============================================================================
DROP VIEW IF EXISTS layer3.dim_products;

CREATE VIEW layer3.dim_products AS
SELECT
    -- A surrogate key for the product dimension.
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

    -- Business keys and descriptive attributes from the product tables.
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM
    layer2.crm_prd_info pn
    -- Joining product info with its category lookup table.
    LEFT JOIN layer2.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
-- This WHERE clause is important: it filters for only the currently active
-- product records, hiding historical data from the main dimension.
WHERE
    pn.prd_end_dt IS NULL; -- get only current products 


-- =============================================================================
-- Create Fact View: layer3.fact_sales
-- Objective: To create a central table of business events (sales transactions).
--            It contains numeric measures and foreign keys to the dimension tables.
-- =============================================================================
DROP VIEW IF EXISTS layer3.fact_sales;

CREATE VIEW layer3.fact_sales AS
SELECT
    -- The business key for the fact table (the order number).
    sd.sls_ord_num AS order_number,

    -- Foreign keys that link this fact table to the dimension tables.
    -- This is the core of the Star Schema model.
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,

    -- Date fields, which can also be linked to a date dimension.
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,

    -- Measurable, numeric data (the "facts").
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM
    layer2.crm_sales_details sd
    -- Joining the sales data to the dimension views to get the surrogate keys.
    LEFT JOIN layer3.dim_products pr ON sd.sls_prd_key = pr.product_number
    LEFT JOIN layer3.dim_customers cu ON sd.sls_cust_id = cu.customer_id;

















