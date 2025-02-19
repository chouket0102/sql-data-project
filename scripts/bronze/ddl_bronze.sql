DELIMITER $$

CREATE PROCEDURE bronze.load_bronze()
BEGIN
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    
    -- Start the batch process
    SET batch_start_time = NOW();
    SELECT '================================================' AS message;
    SELECT 'Loading Bronze Layer' AS message;
    SELECT '================================================' AS message;
    
    -- Load CRM Tables
    SELECT '------------------------------------------------' AS message;
    SELECT 'Loading CRM Tables' AS message;
    SELECT '------------------------------------------------' AS message;

    -- Load crm_cust_info
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_cust_info;
    SELECT '>> Truncating Table: bronze.crm_cust_info' AS message;
    LOAD DATA INFILE '/var/lib/mysql-files/source_crm/cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Load crm_prd_info
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_prd_info;
    SELECT '>> Truncating Table: bronze.crm_prd_info' AS message;
    LOAD DATA INFILE '/var/lib/mysql-files/source_crm/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Load crm_sales_details
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_sales_details;
    SELECT '>> Truncating Table: bronze.crm_sales_details' AS message;
    LOAD DATA INFILE '/var/lib/mysql-files/source_crm/sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Load ERP Tables
    SELECT '------------------------------------------------' AS message;
    SELECT 'Loading ERP Tables' AS message;
    SELECT '------------------------------------------------' AS message;

    -- Load erp_loc_a101
    SET start_time = NOW();
    TRUNCATE TABLE bronze.erp_loc_a101;
    SELECT '>> Truncating Table: bronze.erp_loc_a101' AS message;
    LOAD DATA INFILE '/var/lib/mysql-files/source_erp/loc_a101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Load erp_cust_az12
    SET start_time = NOW();
    TRUNCATE TABLE bronze.erp_cust_az12;
    SELECT '>> Truncating Table: bronze.erp_cust_az12' AS message;
    LOAD DATA INFILE '/var/lib/mysql-files/source_erp/cust_az12.csv'
    INTO TABLE bronze.erp_cust_az12
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Load erp_px_cat_g1v2
    SET start_time = NOW();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    SELECT '>> Truncating Table: bronze.erp_px_cat_g1v2' AS message;
    LOAD DATA INFILE '/var/lib/mysql-files/source_erp/px_cat_g1v2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- End batch process
    SET batch_end_time = NOW();
    SELECT '==========================================' AS message;
    SELECT 'Loading Bronze Layer is Completed' AS message;
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS message;
    SELECT '==========================================' AS message;

END $$

DELIMITER ;
