/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/



create or alter procedure bronze.load_bronze as
begin
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time datetime,@batch_end_time datetime; 
	begin try
		set @batch_start_time=getdate();
		print '**************************************************';

		print 'loading the bronze layer';

		print '**************************************************';

		print '**************************************************';

		print 'loading CRM Table';

		print '**************************************************';

		set @start_time=GETDATE();

		print '>>>Truncating Table:bronze.crm_cust_info';
			truncate table bronze.crm_cust_info

		print '>>>Inserting Table into bronze.crm_cust_info'
			bulk insert bronze.crm_cust_info
			from 'D:\git_project\DataWarehouse\Hint folder\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with(
				firstrow=2,
				fieldterminator= ',',
				tablock
			);
		set @end_time=GETDATE();
		print'>>>Load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) +'Second'
		print'==========================================='

		set @start_time= getdate();
		print '>>>Truncating Table:bronze.crm_prd_info';
			truncate table bronze.crm_prd_info

		print '>>>Inserting Table into bronze.crm_prd_info'
			bulk insert bronze.crm_prd_info
			from 'D:\git_project\DataWarehouse\Hint folder\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
		set @end_time=getdate();
		print 'Load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'Second'
		print '==============================================='

		Set @start_time=GETDATE();
		print '>>>Truncating Table:bronze.crm_sales_details';
			truncate table bronze.crm_sales_details

		print '>>>Inserting Table into bronze.crm_sales_details'
			bulk insert bronze.crm_sales_details
			from 'D:\git_project\DataWarehouse\Hint folder\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			);
		set @end_time=GETDATE();
		print 'Load Duration:' + cast(datediff(second,@start_time,@end_time)as nvarchar) + 'Second'
		print'====================================================='

		print '**************************************************';

		print 'loading ERP Table';

		print '**************************************************';

		Set @start_time=GETDATE();

		print '>>>Truncating Table:bronze.erp_cust_az12';
			truncate table bronze.erp_cust_az12

		print '>>>Inserting Table into bronze.erp_cust_az12'
			bulk insert bronze.erp_cust_az12
			from 'D:\git_project\DataWarehouse\Hint folder\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			);
		 Set @end_time=Getdate();
		 print '>>>Load Duration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'Second'
		 print'===================================================='

		set @start_time=getdate()
		print '>>>Truncating Table:bronze.erp_loc_a101';
			truncate table bronze.erp_loc_a101

		print '>>>Inserting Table into bronze.erp_loc_a101'

			bulk insert bronze.erp_loc_a101
			from 'D:\git_project\DataWarehouse\Hint folder\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			);
		set @end_time = getdate()
		print 'Load Duration:' + cast(datediff(second, @start_time,@end_time) as nvarchar) +'Second'
		print'=========================================================='


		Set @start_time=getdate()
		print '>>>Truncating Table:bronze.erp_px_cat_g1v2';
			truncate table bronze.erp_px_cat_g1v2

		print '>>>Inserting Table into bronze.erp_px_cat_g1v2'
			bulk insert bronze.erp_px_cat_g1v2
			from 'D:\git_project\DataWarehouse\Hint folder\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
		Set @end_time=getdate();
		print '>>>load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'Second'
		print'================================================================'

		set @batch_end_time=getdate();
		print '==============================================================='
		print 'Loading Bronze Layer is Completed'
		print 'Total Load Duration:'+ cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar) +'Second'
		print '==============================================================='

	end try
	begin catch
		print '==========================================='
		print 'ERROR OCCURED DURING LOADING BRONGE LAYER'
		PRINT 'Error message'+ ERROR_MESSAGE();
		PRINT 'Error message'+ cast(error_number() as nvarchar);
		print 'Error message'+ cast(error_state() as nvarchar);
		print '============================================'

	end catch
end
 
