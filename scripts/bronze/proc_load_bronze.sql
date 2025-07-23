/*
===============================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  --Truncates the bronze tables before loading data.
  --Uses the BULK INSERT command to load data from csv Files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze_load_bronze
===============================================================================================================
*/
CREATE OR ALTER PROCEDURE bronze_load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
    SET @batch_start_time = GETDATE();
	PRINT '=====================================================================================================================';
	PRINT 'Loading Bronze Layer';
	PRINT '=====================================================================================================================';

	PRINT '---------------------------------------------------------------------------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '---------------------------------------------------------------------------------------------------------------------';
	
	SET @start_time = GETDATE();
	PRINT '>>Truncating Table: bronze_crm_cst_info';
	TRUNCATE TABLE bronze_crm_cust_info;

	PRINT '>>Inserting Data Into: bronze_crm_cst_info';
	BULK INSERT bronze_crm_cust_info
	FROM 'K:\Desktop\MSSQL16.SQLEXPRESS02\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
	PRINT '>>--------------';

	SET @start_time = GETDATE ();
	PRINT '>>Truncating Table: bronze_crm_prd_info';
	TRUNCATE TABLE bronze_crm_prd_info;

	PRINT '>>Inserting Data Into: bronze_crm_prd_info';
	BULK INSERT bronze_crm_prd_info
	FROM 'K:\Desktop\MSSQL16.SQLEXPRESS02\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
	PRINT '>>--------------';

	SET @start_time = GETDATE ();
	PRINT '>>Truncating Table: bronze_crm_sales_details';
	TRUNCATE TABLE bronze_crm_sales_details;

	PRINT '>>Inserting Data Into: bronze_crm_sales_details';
	BULK INSERT bronze_crm_sales_details
	FROM 'K:\Desktop\MSSQL16.SQLEXPRESS02\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
	PRINT '>>--------------';
	
	SET @start_time = GETDATE ();
	PRINT '>>Truncating Table: bronze_erp_cust_az12';
	TRUNCATE TABLE bronze_erp_cust_az12;

	PRINT '>>Inserting Data Into: bronze_erp_cust_az12';
	BULK INSERT bronze_erp_cust_az12
	FROM 'K:\Desktop\MSSQL16.SQLEXPRESS02\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
	PRINT '>>--------------';


	SET @start_time = GETDATE ();
	PRINT '>>Truncating Table: bronze_erp_loc_a101';
	TRUNCATE TABLE bronze_erp_loc_a101;
	
	PRINT '>>Inserting Data Into: bronze_erp_loc_a101';
	BULK INSERT bronze_erp_loc_a101
	FROM 'K:\Desktop\MSSQL16.SQLEXPRESS02\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
	PRINT '>>--------------';


	SET @start_time = GETDATE ();
	PRINT '>>Truncating Table: bronze_erp_px_cat_g1v2';
	TRUNCATE TABLE bronze_erp_px_cat_g1v2;

	PRINT '>>Inserting Data Into: bronze_erp_px_cat_g1v2';
	BULK INSERT bronze_erp_px_cat_g1v2
	FROM 'K:\Desktop\MSSQL16.SQLEXPRESS02\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
	PRINT '>>--------------';
   SET @batch_end_time = GETDATE();
	PRINT '========================='
	PRINT 'Loading Bronze Layer is Completed';
	PRINT '- Total Load Duration:' +  CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	PRINT '========================='
   
   END TRY

	
	
	BEGIN CATCH
	 PRINT '=====================================================================================================================';
	 PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	 PRINT 'ERROR MESSAGE' + ERROR_MESSAGE ();
	 PRINT 'ERROR MESSAGE' + CAST (ERROR_MESSAGE () AS NVARCHAR);
	 PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE () AS NVARCHAR);
	 PRINT '=====================================================================================================================';
	END CATCH
END
