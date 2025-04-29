/*
==============================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==============================================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.

  Action Performed:
     - Truncate Silver Tables.
     - Insert transformed and cleansed data from Bronze into Silver Tables.

Parameters:
    None.
    This stored procedure does not accept any parameters and return any values.


Usage Example:
      Exec silver.load_silver
==================================================================================================











*/














-- Build silver layer stored procedure

Create or Alter Procedure silver.load_silver As
Begin
    Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
     Begin Try
	    Set @batch_start_time = GETDATE();
		Print '======================================';
		Print 'Loading Silver Layer';
		Print '======================================'

		Print '-----------------------------------------';
		Print 'Loading CRM Tables';
		Print '-----------------------------------------';


		-- Clean Data and Load (crm_cust_info) 

		
		Set @start_time = Getdate();
		Print 'Truncate Table: silver.crm_cust_info';
		Truncate Table silver.crm_cust_info;
		Print 'Insert Into: silver.crm_cust_info';
		Insert Into silver.crm_cust_info(

		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_gndr,
		cst_marital_status,
		cst_create_date

		)
		Select 
		cst_id,
		cst_key,
		-- Removing unwanted spaces
		Trim(cst_firstname) as cst_firstname,
		Trim(cst_lastname) as cst_firstname,
		-- Data normalization and standarization
		-- Handling nulls also 
		Case When UPPER(Trim(cst_marital_status)) = 'S' Then 'Single'-- Normalize material status 
			 When Upper(Trim(cst_marital_status	)) = 'M' Then 'Married'
			 Else 'n/a'
		End cst_material_status,
		Case When UPPER(Trim(cst_gndr)) = 'F' Then 'Female'
			 When Upper(Trim(cst_gndr)) = 'M' Then 'Male'
			 Else 'n/a'
		End as cst_gndr,
		cst_create_date
		From (
		-- Removing duplicates from primary key
		Select *,
		ROW_NUMBER() Over(Partition by cst_id Order by cst_create_date Desc) as Flag_last
		from bronze.crm_cust_info) t
		Where Flag_last =1 ; -- select most recent record per customer
		Set @end_time = Getdate();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';



		--  Clean & Load (crm_prd_info)

		
		Set @start_time = Getdate();
		Print 'Truncate Table: silver.crm_prd_info';
		Truncate Table silver.crm_prd_info;
		Print 'Insert Into: silver.crm_prd_info';
		Insert Into silver.crm_prd_info(

		prd_id,
		cat_id,
		 prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt

		)

		Select 
		prd_id,
		Replace(SUBSTRING(prd_key,1,5),'-','_' ) as cat_id, -- Extract category id 
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,       -- Extract product key 
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost, -- Handling Nulls
		Case 
			 When UPPER(TRIM(prd_line)) = 'M' Then 'Mountain'
			 When UPPER(TRIM(prd_line)) = 'R' Then 'Road'
			 When UPPER(TRIM(prd_line)) = 'S' Then 'Other States'
			 When UPPER(TRIM(prd_line)) = 'T' Then 'Touring'
			 Else 'n/a'
		End as prd_line,-- Map product line code to description value
		Cast(prd_start_dt as date) as prd_start_dt,-- casting data type 
		Cast(
			 LEAD(prd_start_dt) Over (Partition by prd_key Order by prd_start_dt)-1 
			 AS Date
			)as prd_end_dt -- Data Enrichment 
		from bronze.crm_prd_info;
		Set @end_time = Getdate();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';




		-- Clean and Load (crm_sales_details)

		Set @start_time = GETDATE();
		Print 'Truncate Table: silver.crm_sales_details';
		Truncate Table silver.crm_sales_details;
		Print 'Insert Into: silver.crm_sales_details';
		Insert Into silver.crm_sales_details(

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

		Select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			Case When sls_order_dt = 0 OR LEN(sls_order_dt) != 8 Then NUll -- Check the dates and convert it into first varchar then in date format 
				 Else Cast(Cast(sls_order_dt as varchar) as date)
			End sls_order_dt,
			Case When sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 Then NUll -- Check the dates and convert it into first varchar then in date format
				 Else Cast(Cast(sls_ship_dt as varchar) as date)
			End sls_ship_date,
			Case When sls_due_dt = 0 OR LEN(sls_due_dt) != 8 Then NUll -- Check the dates and convert it into first varchar then in date format
				 Else Cast(Cast(sls_due_dt as varchar) as date)
			End sls_due_dt,
			Case When sls_sales IS Null OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price) -- Recalculate the sales if original value is missing or incorrect
					 Then sls_quantity * ABS(sls_price)
				 Else sls_sales
			End as sls_sales,
			sls_quantity,
			Case When sls_price IS Null Or sls_price < =0
				   Then sls_sales/NULLIF(sls_quantity,0) 
				 Else sls_price
			End as sls_price -- Derive price if original value is invalid 
		from bronze.crm_sales_details;
		Set @end_time = Getdate();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';


-- Loading Erp Tables 

		-- Clean and Load (erp_cust_az12)
		Set @start_time = GETDATE();
		Print 'Truncate Table: silver.erp_cust_az12';
		Truncate Table silver.erp_cust_az12;
		Print 'Insert Into: silver.erp_cust_az12';

		Insert Into silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)

		Select 
		Case When cid Like 'NAS%' Then SUBSTRING(cid,4,LEN(cid)) -- Remove 'NAS' prefix if present to extract exact cst_key 
			 Else cid 
		End as cid,
		Case When bdate > GETDATE()  Then Null
			 Else bdate
		End as bdate, -- Set future birthdate is null
		Case When Upper(TRIM(gen)) IN ('F','FEMALE') Then 'Female'
			 When UPPER(TRIM(gen)) IN ('M', 'MALE') Then 'Male'
			 Else 'n/a'
		End as gen -- Normalize gender values and handle unknown cases
		from bronze.erp_cust_az12
		Set @end_time = Getdate();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';



		-- Clean and Load (erp_loc_a101)

		Set @start_time = GETDATE();
		Print 'Truncate Table: silver_erp_loc_a101';
		Truncate Table silver.erp_loc_a101;
		Print 'Insert Into: silver.erp_loc_a101';

		Insert Into silver.erp_loc_a101(
		cid,
		cntry
		)

		Select 
		REPLACE(cid, '-','') as cid, -- Remove '-' from cid to connect with tables
		Case When TRIM(cntry) = 'DE' Then 'Germany'
			 When TRIM(cntry) IN ('US', 'USA') Then 'United States'
			 When TRIM(cntry) = '' OR cntry IS NULL Then 'n/a'
			 Else cntry
		End as cntry -- Normalize and handle missing or blank country codes
		from bronze.erp_loc_a101;
		Set @end_time = Getdate();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';


		-- Clean and Load (erp_px_cat_g1v2)
		Set @start_time = GETDATE();
		Print 'Truncate Table: silver.erp_px_cat_g1v2';
		Truncate Table silver.erp_px_cat_g1v2;
		Print 'Insert Into: silver.erp_px_cat_g1v2';

		Insert Into silver.erp_px_cat_g1v2(

		id,
		cat,
		subcat,
		maintenance

		)

		Select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		Set @end_time = Getdate();
		Print '>> Load Duration:' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) + 'seconds';
		Print '-----------------------------------------------------------------------------------------';

		
		Set @batch_end_time = GETDATE();
		Print '============================================================================================================';
		Print 'Loading Silver Layer Completely';
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


End;
