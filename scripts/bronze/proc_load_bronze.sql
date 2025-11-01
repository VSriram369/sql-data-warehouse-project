CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
 BEGIN TRY
     SET @batch_start_time = GETDATE();
     print '===========================================================';
     PRINT 'Loading Bronze Tables';
	 print '===========================================================';

	 PRINT '===========================================================';
	 PRINT 'Loading CRM Tables';
	 PRINT '===========================================================';

-- Table bronze.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Trunacting bronze.crm_cust_info Table';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting into bronze.crm_cust_info Table';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\srira\OneDrive\Documents\SQL Server Management Studio\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------';

-- Table bronze.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Trunacting bronze.crm_prd_info Table';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting into bronze.crm_prd_info Table';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\srira\OneDrive\Documents\SQL Server Management Studio\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '------------------------------------------------';

-- Table bronze.crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Trunacting bronze.crm_sales_details Table';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting into bronze.crm_sales_details Table';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\srira\OneDrive\Documents\SQL Server Management Studio\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '------------------------------------------------';

	PRINT '===========================================================';
	PRINT 'Loading ERP Tables';
	PRINT '===========================================================';

-- Table bronze.erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Trunacting bronze.erp_cust_az12 Table';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting into bronze.erp_cust_az12 Table';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\srira\OneDrive\Documents\SQL Server Management Studio\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '------------------------------------------------';

-- Table bronze.erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Trunacting bronze.erp_loc_a101 Table';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting into bronze.erp_loc_a101 Table';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\srira\OneDrive\Documents\SQL Server Management Studio\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '------------------------------------------------';

-- Table bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
		PRINT '>> Trunacting bronze.erp_px_cat_g1v2 Table';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting into bronze.erp_px_cat_g1v2 Table';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\srira\OneDrive\Documents\SQL Server Management Studio\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '------------------------------------------------';

		SET @batch_end_time = GETDATE();
		print '------------------------------------------------';
		PRINT 'LOADING BRONZE LAYER IS COMPLETED';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		print '------------------------------------------------';
	END TRY
	BEGIN CATCH
	     PRINT '===========================================================';
		 PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		 PRINT 'Error Message' + ERROR_MESSAGE();
		 PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
		 PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR);
		 PRINT '===========================================================';
	END CATCH
END

-- EXEC bronze.load_bronze 
