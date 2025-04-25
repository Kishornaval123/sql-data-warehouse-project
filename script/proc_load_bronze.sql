/*
========================================================================================
Stored Procedure : Load Bronze Layer (Source -> Bronze)
========================================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It perform the following actions:
    - Truncate the bronze tables before loading data.
    - Uses 'BULK INSERT' command to load data from csv files to bronze tables

Parameters:
      None.
  This stored procedure does not accept any paramenters or return any values.

Usage Example:
     Exec bronze.load_bronze
===========================================================================================

*/




Use DataWarehouse;


-- Create Stored Procedure for Automation
Create Or Alter Procedure bronze.load_bronze AS
Begin
     Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
     Begin Try
	    Set @batch_start_time = GETDATE();
		Print '======================================';
		Print 'Loading Bronze Layer';
		Print '======================================'

		Print '-----------------------------------------';
		Print 'Loading CRM Tables';
		Print '-----------------------------------------';

		-- Bulk Loading Data into cust_info
		-- Full Load(Truncate & Insert) of crm_cust_info

		Set @start_time = Getdate();
		Print '>> Truncating Table: bronze.crm_cust_info';
		Truncate Table bronze.crm_cust_info;
		Print '>> Inserting Data Into: bronze.crm_cust_info';
		Bulk Insert bronze.crm_cust_info
		From  'D:\Data Warehouse\Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With (
			  Firstrow =2,
			  Fieldterminator = ',',
			  Tablock
		);
		Set @end_time = Getdate();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';

		-- Full Load(Truncate & Insert) of crm_prd_info
	    Set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_prd_info';
		Truncate Table bronze.crm_prd_info;

		Print '>> Inserting Data Into: bronze.crm_prd_info';
		Bulk Insert bronze.crm_prd_info
		From  'D:\Data Warehouse\Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With (
			  Firstrow =2,
			  Fieldterminator = ',',
			  Tablock
		);
		Set @end_time = GETDATE();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';
		
		-- Full Load(Truncate & Insert) of crm_sales_details
		Set @start_time =GETDATE();
		Print '>> Truncating Table: bronze.crm_sales_details';
		Truncate Table bronze.crm_sales_details;

		Print '>> Inserting Data Into: bronze.crm_sales_details';
		Bulk Insert bronze.crm_sales_details
		From  'D:\Data Warehouse\Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With (
			  Firstrow =2,
			  Fieldterminator = ',',
			  Tablock
		);
		Set @end_time = GETDATE();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';

	-- Loading of ERP Tables

		Print '-----------------------------------------';
		Print 'Loading ERP Tables';
		Print '-----------------------------------------';

		-- Full Load (Truncate & Insert) of erp_cust_az12
		Set @start_time = GETDATE(); 
		Print '>> Truncating Table: bronze.erp_cust_az12';
		Truncate Table bronze.erp_cust_az12;

		Print '>> Inserting Data Into: bronze.erp_cust_az12';
		Bulk Insert bronze.erp_cust_az12
		From  'D:\Data Warehouse\Warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		With (
			  Firstrow =2,
			  Fieldterminator = ',',
			  Tablock
		);
		Set @end_time = GETDATE();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';

		-- Full Load (Truncate & Insert) of erp_loc_a101
		Set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_loc_a101';
		Truncate Table bronze.erp_loc_a101;

		Print '>> Inserting Data Into: bronze.erp_loc_a101';
		Bulk Insert bronze.erp_loc_a101
		From  'D:\Data Warehouse\Warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		With (
			  Firstrow =2,
			  Fieldterminator = ',',
			  Tablock
		);
		Set @end_time = GETDATE();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';

		-- Full Load (Truncate & Insert) of erp_px_cat_g1v2

		Set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		Truncate Table bronze.erp_px_cat_g1v2;

		Print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		Bulk Insert bronze.erp_px_cat_g1v2
		From  'D:\Data Warehouse\Warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		With (
			  Firstrow =2,
			  Fieldterminator = ',',
			  Tablock
		);
		Set @end_time = GETDATE();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';

		Set @batch_end_time = GETDATE();
		Print '============================================================================================================';
		Print 'Loading Bronze Layer Completely';
		Print '>> Total Load Duration:' + Cast(Datediff(second,@batch_start_time,@batch_end_time) as Nvarchar) + 'seconds';
		Print '============================================================================================================';
    End Try 
	Begin Catch
	    Print '===========================================';

		Print 'Error Occured During Loading Bronze Layer';
		Print 'Error Message ' + Error_Message();
		Print 'Error Number' + Cast(Error_number()as Nvarchar);
		Print 'Error State' +  Cast(Error_State() as Nvarchar);

		Print '===========================================';
	End Catch 
End


