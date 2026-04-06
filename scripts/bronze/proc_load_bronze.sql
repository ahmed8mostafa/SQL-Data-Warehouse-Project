/*
====================================================================
Stored Procedure: Load Bronze Layer (Source → Bronze)
====================================================================

Purpose:
Loads raw data into the 'bronze' schema from external CSV source files.

Process Overview:
- Truncates existing bronze tables before loading data.
- Uses BULK INSERT to ingest CRM and ERP datasets.
- Tracks execution time for each table load and the full batch.

Execution:
EXEC bronze.load_bronze;

Notes:
This procedure accepts no parameters and returns no values.
====================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS


BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @Batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		print '================================';
		print 'Loading Bronze Layer';
		print '================================';
		print '--------------------------------';
		print 'Loading CRM Tables';
		print '--------------------------------';
		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_info'

		TRUNCATE TABLE bronze.crm_cust_info

		print '>> Inserting Data Into: bronze.crm_cust_info'

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\SQLprojects\MAT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '>> -------------'



		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_info'

		TRUNCATE TABLE bronze.crm_prd_info

		print '>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\SQLprojects\MAT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '>> -------------'




		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details'

		TRUNCATE TABLE bronze.crm_sales_details

		print '>> Inserting Data Into: bronze.crm_sales_details'

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\SQLprojects\MAT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '>> -------------'





		print '--------------------------------';
		print 'Loading ERP Tables';
		print '--------------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_az12'


		TRUNCATE TABLE bronze.erp_cust_az12

		print '>> Inserting Data Into: bronze.erp_cust_az12'

		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\SQLprojects\MAT\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '>> -------------'







		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_loc_a101'

		TRUNCATE TABLE bronze.erp_cust_loc_a101

		print '>> Inserting Data Into: bronze.erp_cust_loc_a101'

		BULK INSERT bronze.erp_cust_loc_a101
		FROM 'D:\SQLprojects\MAT\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '>> -------------'








		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2'

		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2'

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\SQLprojects\MAT\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '>> -------------'
		SET @Batch_end_time = GETDATE()
		print '====================================='
		print 'Loading Bronze Layer is Completed';
		print '    - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @Batch_end_time) AS NVARCHAR) + 'seconds';




	END TRY
	BEGIN CATCH
		PRINT '==========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================='
	END CATCH


END


