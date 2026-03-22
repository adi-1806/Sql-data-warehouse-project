/*
======================================================================
DDL Script: Create Gold Views
======================================================================

Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema).

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.

======================================================================
*/

/* 
1st Object/View: Customer

*/
DROP VIEW IF EXISTS DWBarra_gold.dim_customers;

CREATE VIEW DWBarra_gold.dim_customers AS 
select
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE coalesce(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date 
FROM DWBarra_silver.crm_cust_info ci 
LEFT JOIN DWBarra_silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN DWBarra_silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

/* 

2nd Object/View: Product

*/

DROP VIEW IF EXISTS DWBarra_gold.dim_products;

CREATE VIEW DWBarra_gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id As product_id,
    pn.prd_key As product_number,
    pn.prd_nm As product_name,
	pn.cat_id As category_id,
	pc.cat As category,
    pc.subcat As subcategory,
    pc.maintenance,
	pn.prd_cost As cost,
	pn.prd_line As product_line,
	pn.prd_start_dt As start_date
FROM DWBarra_silver.crm_prd_info pn
LEFT JOIN DWBarra_silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL; -- filter out all historical data

/* 

3rd Object/View: Sales

*/

DROP VIEW IF EXISTS DWBarra_gold.fact_sales;

CREATE VIEW DWBarra_gold.fact_sales AS 
SELECT
    sd.sls_ord_num As order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
FROM DWBarra_silver.crm_sales_details sd
LEFT JOIN DWBarra_gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number
LEFT JOIN DWBarra_gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id



