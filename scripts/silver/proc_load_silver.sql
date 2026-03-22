
/*
======================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL DWBarra_silver.load_silver;

======================================================================
*/

DROP PROCEDURE IF EXISTS DWBarra_silver.load_silver;
DELIMITER $$

CREATE PROCEDURE DWBarra_silver.load_silver()
BEGIN
    -- --------------------------------------------------------------
    -- Variables for batch timing
    -- --------------------------------------------------------------
    DECLARE v_batch_start DATETIME(6);
    DECLARE v_batch_end   DATETIME(6);

	-- --------------------------------------------------------------
    -- Variables for step timing
    -- --------------------------------------------------------------
    DECLARE v_step_start DATETIME(6);
    DECLARE v_step_end   DATETIME(6);

    -- --------------------------------------------------------------
    -- Variables for error handling
    -- --------------------------------------------------------------
    DECLARE v_sqlstate CHAR(5) DEFAULT '00000';
    DECLARE v_errno INT DEFAULT 0;
    DECLARE v_message TEXT;

    -- --------------------------------------------------------------
    -- Error handler
    -- --------------------------------------------------------------
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            v_sqlstate = RETURNED_SQLSTATE,
            v_errno    = MYSQL_ERRNO,
            v_message  = MESSAGE_TEXT;

        SET v_batch_end = NOW(6);

        SELECT 'ERROR: Silver load failed' AS message;
        SELECT v_sqlstate AS err_sqlstate, v_errno AS error_no, v_message AS error_message;
        SELECT v_batch_start AS batch_start_time,
               v_batch_end   AS batch_end_time,
               TIMESTAMPDIFF(MICROSECOND, v_batch_start, v_batch_end) / 1000000 AS total_duration_seconds;
    END;

	-- --------------------------------------------------------------
    -- Batch start
    -- --------------------------------------------------------------
    SET v_batch_start = NOW(6);

    SELECT '========================================' AS message;
    SELECT 'Starting Silver Layer Load' AS message;
    SELECT v_batch_start AS batch_start_time;
    SELECT '========================================' AS message;

    /****************************************************************
        SECTION 1: CRM TABLES
    ****************************************************************/
    SELECT 'SECTION 1: Loading CRM Tables' AS message;

   -- --------------------------------------------------------------
    -- Step 1: crm_cust_info
   -- --------------------------------------------------------------
    SET v_step_start = NOW(6);
    SELECT 'Step 1: Truncating DWBarra_silver.crm_cust_info' AS message;

    TRUNCATE TABLE DWBarra_silver.crm_cust_info;

    SELECT 'Step 1: Inserting into DWBarra_silver.crm_cust_info' AS message;

    INSERT INTO DWBarra_silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date
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
        END AS cst_material_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        CASE
            WHEN CAST(cst_create_date AS CHAR) = '0000-00-00' THEN NULL
            ELSE cst_create_date
        END AS cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM DWBarra_bronze.crm_cust_info
    ) t
    WHERE flag_last = 1
      AND cst_id <> 0;

    SET v_step_end = NOW(6);

    SELECT 'Step 1 completed: crm_cust_info loaded' AS message,
           v_step_start AS step_start_time,
           v_step_end AS step_end_time,
           TIMESTAMPDIFF(MICROSECOND, v_step_start, v_step_end) / 1000000 AS duration_seconds;

    -- --------------------------------------------------------------
    -- Step 2: crm_prd_info
    -- --------------------------------------------------------------
    SET v_step_start = NOW(6);
    SELECT 'Step 2: Truncating DWBarra_silver.crm_prd_info' AS message;

    TRUNCATE TABLE DWBarra_silver.crm_prd_info;

    SELECT 'Step 2: Inserting into DWBarra_silver.crm_prd_info' AS message;

    INSERT INTO DWBarra_silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7) AS prd_key,
        prd_nm,
        prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            DATE_SUB(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ),
                INTERVAL 1 DAY
            ) AS DATE
        ) AS prd_end_dt
    FROM DWBarra_bronze.crm_prd_info;

    SET v_step_end = NOW(6);

    SELECT 'Step 2 completed: crm_prd_info loaded' AS message,
           v_step_start AS step_start_time,
           v_step_end AS step_end_time,
           TIMESTAMPDIFF(MICROSECOND, v_step_start, v_step_end) / 1000000 AS duration_seconds;

    -- --------------------------------------------------------------
    -- Step 3: crm_sales_details
    -- --------------------------------------------------------------
    SET v_step_start = NOW(6);
    SELECT 'Step 3: Truncating DWBarra_silver.crm_sales_details' AS message;

    TRUNCATE TABLE DWBarra_silver.crm_sales_details;

    SELECT 'Step 3: Inserting into DWBarra_silver.crm_sales_details' AS message;

    INSERT INTO DWBarra_silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        CASE
            WHEN sls_order_dt = 0
              OR LENGTH(CAST(sls_order_dt AS CHAR)) <> 8
            THEN NULL
            ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
        END AS sls_order_dt,

        CASE
            WHEN sls_ship_dt = 0
              OR LENGTH(CAST(sls_ship_dt AS CHAR)) <> 8
            THEN NULL
            ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
        END AS sls_ship_dt,

        CASE
            WHEN sls_due_dt = 0
              OR LENGTH(CAST(sls_due_dt AS CHAR)) <> 8
            THEN NULL
            ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
        END AS sls_due_dt,

        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,

        sls_quantity,

        CASE
            WHEN sls_price IS NULL
              OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM DWBarra_bronze.crm_sales_details;

    SET v_step_end = NOW(6);

    SELECT 'Step 3 completed: crm_sales_details loaded' AS message,
           v_step_start AS step_start_time,
           v_step_end AS step_end_time,
           TIMESTAMPDIFF(MICROSECOND, v_step_start, v_step_end) / 1000000 AS duration_seconds;

    /****************************************************************
        SECTION 2: ERP TABLES
    ****************************************************************/
    SELECT 'SECTION 2: Loading ERP Tables' AS message;

    -- --------------------------------------------------------------
    -- Step 4: erp_cust_az12
    -- --------------------------------------------------------------
    SET v_step_start = NOW(6);
    SELECT 'Step 4: Truncating DWBarra_silver.erp_cust_az12' AS message;

    TRUNCATE TABLE DWBarra_silver.erp_cust_az12;

    SELECT 'Step 4: Inserting into DWBarra_silver.erp_cust_az12' AS message;

    INSERT INTO DWBarra_silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > CURRENT_DATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM DWBarra_bronze.erp_cust_az12;

    SET v_step_end = NOW(6);

    SELECT 'Step 4 completed: erp_cust_az12 loaded' AS message,
           v_step_start AS step_start_time,
           v_step_end AS step_end_time,
           TIMESTAMPDIFF(MICROSECOND, v_step_start, v_step_end) / 1000000 AS duration_seconds;

    -- --------------------------------------------------------------
    -- Step 5: erp_loc_a101
    -- --------------------------------------------------------------
    SET v_step_start = NOW(6);
    SELECT 'Step 5: Truncating DWBarra_silver.erp_loc_a101' AS message;

    TRUNCATE TABLE DWBarra_silver.erp_loc_a101;

    SELECT 'Step 5: Inserting into DWBarra_silver.erp_loc_a101' AS message;

    INSERT INTO DWBarra_silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM DWBarra_bronze.erp_loc_a101;

    SET v_step_end = NOW(6);

    SELECT 'Step 5 completed: erp_loc_a101 loaded' AS message,
           v_step_start AS step_start_time,
           v_step_end AS step_end_time,
           TIMESTAMPDIFF(MICROSECOND, v_step_start, v_step_end) / 1000000 AS duration_seconds;

    -- --------------------------------------------------------------
    -- Step 6: erp_px_cat_g1v2
    -- --------------------------------------------------------------
    SET v_step_start = NOW(6);
    SELECT 'Step 6: Truncating DWBarra_silver.erp_px_cat_g1v2' AS message;

    TRUNCATE TABLE DWBarra_silver.erp_px_cat_g1v2;

    SELECT 'Step 6: Inserting into DWBarra_silver.erp_px_cat_g1v2' AS message;

    INSERT INTO DWBarra_silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM DWBarra_bronze.erp_px_cat_g1v2;

    SET v_step_end = NOW(6);

    SELECT 'Step 6 completed: erp_px_cat_g1v2 loaded' AS message,
           v_step_start AS step_start_time,
           v_step_end AS step_end_time,
           TIMESTAMPDIFF(MICROSECOND, v_step_start, v_step_end) / 1000000 AS duration_seconds;

    -- --------------------------------------------------------------
    -- Batch completed
    -- --------------------------------------------------------------
    SET v_batch_end = NOW(6);

    SELECT '========================================' AS message;
    SELECT 'Silver Layer Load Completed Successfully' AS message;
    SELECT v_batch_start AS batch_start_time,
           v_batch_end AS batch_end_time,
           TIMESTAMPDIFF(MICROSECOND, v_batch_start, v_batch_end) / 1000000 AS total_duration_seconds;
    SELECT '========================================' AS message;

END $$

DELIMITER ;
