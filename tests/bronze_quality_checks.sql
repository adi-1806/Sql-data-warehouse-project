/*=====================================================================
  BRONZE LAYER - DATA QUALITY CHECKS
  Purpose:
      Validate raw Bronze layer data before transformation into Silver.
=====================================================================*/


/*=====================================================================
  1. CRM_CUSTOMER_INFO
=====================================================================*/

-- Check duplicate customer IDs in Bronze
SELECT
    cst_id,
    COUNT(*) AS record_count
FROM DWBarra_bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check latest record retained per customer
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM DWBarra_bronze.crm_cust_info
) t
WHERE flag_last = 1;

-- Check for unwanted spaces in customer name fields
SELECT *
FROM DWBarra_bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
   OR cst_lastname  <> TRIM(cst_lastname);

-- Check marital status standardization in Bronze
SELECT DISTINCT cst_marital_status
FROM DWBarra_bronze.crm_cust_info
ORDER BY cst_marital_status;

-- Check gender standardization in Bronze
SELECT DISTINCT cst_gndr
FROM DWBarra_bronze.crm_cust_info
ORDER BY cst_gndr;

-- Check invalid customer creation dates in Bronze
SELECT *
FROM DWBarra_bronze.crm_cust_info
WHERE CAST(cst_create_date AS CHAR) = '0000-00-00'
   OR cst_create_date > CURRENT_DATE;

-- Check null / missing important customer fields
SELECT *
FROM DWBarra_bronze.crm_cust_info
WHERE cst_id IS NULL
   OR cst_key IS NULL
   OR cst_firstname IS NULL
   OR cst_lastname IS NULL;



/*=====================================================================
  2. CRM_PRODUCT_INFO
=====================================================================*/

-- Check duplicate product IDs in Bronze
SELECT
    prd_id,
    COUNT(*) AS record_count
FROM DWBarra_bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces in product fields
SELECT *
FROM DWBarra_bronze.crm_prd_info
WHERE prd_nm   <> TRIM(prd_nm)
   OR prd_key  <> TRIM(prd_key)
   OR prd_line <> TRIM(prd_line);

-- Check raw product line values in Bronze
SELECT DISTINCT prd_line
FROM DWBarra_bronze.crm_prd_info
ORDER BY prd_line;

-- Check invalid or negative product cost
SELECT *
FROM DWBarra_bronze.crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;

-- Check product start dates
SELECT *
FROM DWBarra_bronze.crm_prd_info
WHERE prd_start_dt IS NULL
   OR prd_start_dt > CURRENT_DATE;



/*=====================================================================
  3. CRM_SALES_DETAILS
=====================================================================*/

-- Check duplicate sales order numbers in Bronze
SELECT
    sls_ord_num,
    COUNT(*) AS record_count
FROM DWBarra_bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1 OR sls_ord_num IS NULL;

-- Check missing important sales keys
SELECT *
FROM DWBarra_bronze.crm_sales_details
WHERE sls_ord_num IS NULL
   OR sls_prd_key IS NULL
   OR sls_cust_id IS NULL;

-- Check invalid sales dates in Bronze
SELECT *
FROM DWBarra_bronze.crm_sales_details
WHERE sls_order_dt = 0
   OR sls_ship_dt = 0
   OR sls_due_dt = 0;

-- Check raw sales / quantity / price issues in Bronze
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM DWBarra_bronze.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
   OR sls_sales <> sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price;



/*=====================================================================
  4. ERP_CUSTOMER_AZ12
=====================================================================*/

-- Check out-of-range birthdates in Bronze
SELECT DISTINCT bdate
FROM DWBarra_bronze.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > CURRENT_DATE;

-- Check raw gender values in Bronze
SELECT DISTINCT gen
FROM DWBarra_bronze.erp_cust_az12
ORDER BY gen;

-- Check missing customer IDs in Bronze
SELECT *
FROM DWBarra_bronze.erp_cust_az12
WHERE cid IS NULL
   OR TRIM(cid) = '';



/*=====================================================================
  5. ERP_LOCATION_A101
=====================================================================*/

-- Check country standardization in Bronze
SELECT DISTINCT cntry
FROM DWBarra_bronze.erp_loc_a101
ORDER BY cntry;

-- Check unwanted spaces in country values
SELECT *
FROM DWBarra_bronze.erp_loc_a101
WHERE cntry <> TRIM(cntry);

-- Check missing customer IDs or country values
SELECT *
FROM DWBarra_bronze.erp_loc_a101
WHERE cid IS NULL
   OR TRIM(cid) = ''
   OR cntry IS NULL
   OR TRIM(cntry) = '';



/*=====================================================================
  6. ERP_PRODUCT_CATEGORY_G1V2
=====================================================================*/

-- Check for unwanted spaces in Bronze
SELECT *
FROM DWBarra_bronze.erp_px_cat_g1v2
WHERE cat         <> TRIM(cat)
   OR subcat      <> TRIM(subcat)
   OR maintenance <> TRIM(maintenance);

-- Check standardization of category values in Bronze
SELECT DISTINCT cat
FROM DWBarra_bronze.erp_px_cat_g1v2
ORDER BY cat;

SELECT DISTINCT subcat
FROM DWBarra_bronze.erp_px_cat_g1v2
ORDER BY subcat;

SELECT DISTINCT maintenance
FROM DWBarra_bronze.erp_px_cat_g1v2
ORDER BY maintenance;

-- Check missing values in Bronze
SELECT *
FROM DWBarra_bronze.erp_px_cat_g1v2
WHERE id IS NULL
   OR cat IS NULL
   OR subcat IS NULL
   OR maintenance IS NULL;

-- Check duplicate IDs in Bronze
SELECT
    id,
    COUNT(*) AS record_count
FROM DWBarra_bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL;
