/*
DDL Script: Create Gold Views
=================================================================================================================================================
Script Purpose:
  This script creates views for the Gold layer in the data warehouse. The Gold layer represents the final dimension and fact tables (Star Schema)
  Each view performs transformations and combines data from the Silver layer to produce a clean, enriched, and business-ready dataset.

Usage:
  -These views can be queried directly for analytics and reporting.
=================================================================================================================================================
*/
--===================================================
-- Create Dimension: gold_dim_customers
--===================================================
IF OBJECT_ID ('gold_dim_customers', 'V') IS NOT NULL
    DROP VIEW gold_dim_customers;
GO

CREATE VIEW gold_dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr  -- CRM is the Master for gender info
        ELSE COALESCE(ca.gen, 'N/A')
    END AS gender,
    ci.cst_create AS create_date,
    ca.bdate AS birthdate,
    la.cntry AS country
FROM silver_crm_cust_info ci
LEFT JOIN silver_erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

--===================================================
-- Create Dimension: gold_dim_products
--===================================================
IF OBJECT_ID('gold_dim_products', 'V') IS NOT NULL
    DROP VIEW gold_dim_products;
GO

CREATE VIEW gold_dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver_crm_prd_info pn
LEFT JOIN silver_erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;  -- Filter out historical data
GO

--===================================================
-- Create Fact: gold_fact_sales
--===================================================
IF OBJECT_ID('gold_fact_sales', 'V') IS NOT NULL
    DROP VIEW gold_fact_sales;
GO

CREATE VIEW gold_fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_id,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS sales_price
FROM silver_crm_sales_details sd
LEFT JOIN gold_dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO

