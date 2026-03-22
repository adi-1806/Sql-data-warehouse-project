/*=====================================================================
  FILE: silver_quality_checks.sql

  PURPOSE:
      Validate data quality in the Silver layer after transformation.

  CHECKS INCLUDED:
      - duplicates
      - nulls / missing values
      - invalid dates
      - business rule violations
      - standardization checks
      - unwanted spaces
      - data consistency checks
=====================================================================*/


/*=====================================================================
  1. CRM_CUSTOMER_INFO
=====================================================================*/

-- --------------------------------------------------------------------
-- 1.1 Check duplicate customer IDs in Silver
-- Expectation:
--     cst_id should be unique and not null.
-- --------------------------------------------------------------------
SELECT
    cst_id,
    COUNT(*) AS record_count
FROM DWBarra_silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- --------------------------------------------------------------------
-- 1.2 Check missing important customer fields
-- Expectation:
--     Key customer columns should not be null.
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_cust_info
WHERE cst_id IS NULL
   OR cst_key IS NULL
   OR cst_firstname IS NULL
   OR cst_lastname IS NULL;


-- --------------------------------------------------------------------
-- 1.3 Check for unwanted spaces in customer name fields
-- Expectation:
--     No rows should be returned.
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
   OR cst_lastname  <> TRIM(cst_lastname);


-- --------------------------------------------------------------------
-- 1.4 Check standardized marital status values
-- Expectation:
--     Single, Married, n/a
-- --------------------------------------------------------------------
SELECT DISTINCT cst_marital_status
FROM DWBarra_silver.crm_cust_info
ORDER BY cst_marital_status;


-- --------------------------------------------------------------------
-- 1.5 Check standardized gender values
-- Expectation:
--     Female, Male, n/a
-- --------------------------------------------------------------------
SELECT DISTINCT cst_gndr
FROM DWBarra_silver.crm_cust_info
ORDER BY cst_gndr;


-- --------------------------------------------------------------------
-- 1.6 Check invalid future customer create dates
-- Expectation:
--     No future dates.
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_cust_info
WHERE cst_create_date > CURRENT_DATE;


-- --------------------------------------------------------------------
-- 1.7 View final Silver customer data
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_cust_info;



/*=====================================================================
  2. CRM_PRODUCT_INFO
=====================================================================*/

-- --------------------------------------------------------------------
-- 2.1 Check duplicate product IDs in Silver
-- Expectation:
--     prd_id should be unique and not null.
-- --------------------------------------------------------------------
SELECT
    prd_id,
    COUNT(*) AS record_count
FROM DWBarra_silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- --------------------------------------------------------------------
-- 2.2 Check missing important product fields
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_prd_info
WHERE prd_id IS NULL
   OR prd_key IS NULL
   OR prd_nm IS NULL;


-- --------------------------------------------------------------------
-- 2.3 Check standardized product line values
-- Expectation:
--     Mountain, Road, Other Sales, Touring, n/a
-- --------------------------------------------------------------------
SELECT DISTINCT prd_line
FROM DWBarra_silver.crm_prd_info
ORDER BY prd_line;


-- --------------------------------------------------------------------
-- 2.4 Check invalid negative product cost
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;


-- --------------------------------------------------------------------
-- 2.5 Check invalid product date ranges
-- Expectation:
--     prd_end_dt should be after or equal to prd_start_dt
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL
  AND prd_end_dt < prd_start_dt;


-- --------------------------------------------------------------------
-- 2.6 Check future product start dates
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_prd_info
WHERE prd_start_dt > CURRENT_DATE;


-- --------------------------------------------------------------------
-- 2.7 View final Silver product data
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_prd_info;



/*=====================================================================
  3. CRM_SALES_DETAILS
=====================================================================*/

-- --------------------------------------------------------------------
-- 3.1 Check missing important sales fields
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_sales_details
WHERE sls_ord_num IS NULL
   OR sls_prd_key IS NULL
   OR sls_cust_id IS NULL;


-- --------------------------------------------------------------------
-- 3.2 Check invalid sales date sequence
-- Expectation:
--     Order date should not be after ship date or due date.
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;


-- --------------------------------------------------------------------
-- 3.3 Check sales calculation consistency
-- Expectation:
--     sls_sales = sls_quantity * sls_price
-- --------------------------------------------------------------------
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM DWBarra_silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- --------------------------------------------------------------------
-- 3.4 Check for null sales dates after transformation
-- Purpose:
--     Review rows with invalid dates converted to NULL.
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_ship_dt IS NULL
   OR sls_due_dt IS NULL;


-- --------------------------------------------------------------------
-- 3.5 View final Silver sales data
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.crm_sales_details;



/*=====================================================================
  4. ERP_CUSTOMER_AZ12
=====================================================================*/

-- --------------------------------------------------------------------
-- 4.1 Check missing customer IDs
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.erp_cust_az12
WHERE cid IS NULL
   OR TRIM(cid) = '';


-- --------------------------------------------------------------------
-- 4.2 Check out-of-range birthdates
-- Expectation:
--     Birthdate should not be too old or in the future.
-- --------------------------------------------------------------------
SELECT DISTINCT bdate
FROM DWBarra_silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > CURRENT_DATE;


-- --------------------------------------------------------------------
-- 4.3 Check standardized gender values
-- Expectation:
--     Female, Male, n/a
-- --------------------------------------------------------------------
SELECT DISTINCT gen
FROM DWBarra_silver.erp_cust_az12
ORDER BY gen;


-- --------------------------------------------------------------------
-- 4.4 View final Silver ERP customer data
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.erp_cust_az12;



/*=====================================================================
  5. ERP_LOCATION_A101
=====================================================================*/

-- --------------------------------------------------------------------
-- 5.1 Check missing customer IDs or country values
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.erp_loc_a101
WHERE cid IS NULL
   OR TRIM(cid) = ''
   OR cntry IS NULL
   OR TRIM(cntry) = '';


-- --------------------------------------------------------------------
-- 5.2 Check unwanted spaces in country values
-- Expectation:
--     No rows should be returned.
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.erp_loc_a101
WHERE cntry <> TRIM(cntry);


-- --------------------------------------------------------------------
-- 5.3 Check standardized country values
-- Expectation:
--     Germany, United States, n/a, or cleaned country names
-- --------------------------------------------------------------------
SELECT DISTINCT cntry
FROM DWBarra_silver.erp_loc_a101
ORDER BY cntry;


-- --------------------------------------------------------------------
-- 5.4 View final Silver ERP location data
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.erp_loc_a101;



/*=====================================================================
  6. ERP_PRODUCT_CATEGORY_G1V2
=====================================================================*/

-- --------------------------------------------------------------------
-- 6.1 Check duplicate IDs
-- --------------------------------------------------------------------
SELECT
    id,
    COUNT(*) AS record_count
FROM DWBarra_silver.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL;


-- --------------------------------------------------------------------
-- 6.2 Check missing values
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.erp_px_cat_g1v2
WHERE id IS NULL
   OR cat IS NULL
   OR subcat IS NULL
   OR maintenance IS NULL;


-- --------------------------------------------------------------------
-- 6.3 Check unwanted spaces
-- Expectation:
--     No rows should be returned.
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.erp_px_cat_g1v2
WHERE cat         <> TRIM(cat)
   OR subcat      <> TRIM(subcat)
   OR maintenance <> TRIM(maintenance);


-- --------------------------------------------------------------------
-- 6.4 Check category standardization
-- --------------------------------------------------------------------
SELECT DISTINCT cat
FROM DWBarra_silver.erp_px_cat_g1v2
ORDER BY cat;

SELECT DISTINCT subcat
FROM DWBarra_silver.erp_px_cat_g1v2
ORDER BY subcat;

SELECT DISTINCT maintenance
FROM DWBarra_silver.erp_px_cat_g1v2
ORDER BY maintenance;


-- --------------------------------------------------------------------
-- 6.5 View final Silver ERP product category data
-- --------------------------------------------------------------------
SELECT *
FROM DWBarra_silver.erp_px_cat_g1v2;
