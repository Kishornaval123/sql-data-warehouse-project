/*
===========================================================================================
Quality Checks 
===========================================================================================

Script Purpose:
     This script perform various checks for data consistency, accuracy and standarization
     across the 'silver' schema. It includes checks for:
     -- Null or duplicates from primary key
     -- Unwanted spaces from string fields 
     -- Data standrization and consistency 
     -- Invalid dates ranges and order 
     -- Data consistency between related fields


Usage Notes:
     - Run these check after data loading silver layer.
     - Investigate and resolve any discrepancies found during checks.
=============================================================================================














*/







-- Quality Checks 
-- Checking Primary key duplication

Select prd_id,
COUNT(*)
from silver.crm_prd_info
Group by prd_id
Having COUNT(*) > 1 or prd_id is null

--Check unwanted spaces in Columns
-- Expectation : No Result

Select prd_nm
from silver.crm_prd_info
Where prd_nm != TRIM(prd_nm);


Select * from bronze.crm_prd_info


-- Check for nulls and negative no
-- Expectation : No Result

Select prd_cost 
from silver.crm_prd_info
Where prd_cost < 0 or prd_cost is null



-- Remove unwanted spaces

Select 
cst_firstname
from silver.crm_cust_info
Where cst_firstname != TRIM(cst_firstname); 

-- Data Standardization and Consistency 

Select distinct prd_line
from silver.crm_prd_info;

Select distinct cst_material_status
from silver.crm_cust_info;

Select * from silver.crm_cust_info;

Exec bronze.load_bronze;

Select distinct prd_line 
from bronze.crm_prd_info;




-- Check  for invalid dates orders

Select * 
from silver.crm_prd_info
Where prd_end_dt < prd_start_dt


Select * from silver.crm_prd_info;


Select  * from silver.crm_cust_info;

-- Correct invalid date 

Use DataWarehouse

Select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) Over (Partition by prd_key Order by prd_start_dt)-1 as prd_start_dt_test
from bronze.crm_prd_info
Where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


-- Check Invalid dates 

Select 
Nullif(sls_due_dt,0) as sls_due_dt
from bronze.crm_sales_details
Where sls_due_dt <=0 
or LEN(sls_due_dt) != 8
or sls_due_dt > 20500101
or sls_due_dt < 19000101;


Select Count(*) from bronze.crm_sales_details;


Select * 
from silver.crm_sales_details
Where sls_order_dt > sls_ship_dt Or sls_order_dt > sls_due_dt;


-- Check data consistency between: sales, quantity and price
-- >> sales = quantity * price
-- >> Values must not be NUll, Zero or negative

Select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
Case When sls_sales IS Null OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price)
         Then sls_quantity * ABS(sls_price)
	 Else sls_sales
End as sls_sales,
Case When sls_price IS Null Or sls_price < =0
       Then sls_sales/NULLIF(sls_quantity,0) 
     Else sls_price
End as sls_price
from  bronze.crm_sales_details

Where sls_sales != sls_quantity*sls_price
Or sls_sales is null or sls_quantity is null or sls_price is null
Or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
Order by sls_sales,sls_quantity,sls_price;


Select * from silver.crm_sales_details;





Select distinct
sls_sales,
sls_quantity,
sls_price
from  silver.crm_sales_details
Where sls_sales != sls_quantity*sls_price
Or sls_sales is null or sls_quantity is null or sls_price is null
Or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
Order by sls_sales,sls_quantity,sls_price;



Use DataWarehouse
-- Clean and Load

Select 
bdate
from silver.erp_cust_az12
Where bdate < '1924-01-01' 
Or bdate > '2050-01-01';


Select distinct 
gen,
Case When Upper(TRIM(gen)) IN ('F','FEMALE') Then 'Female'
     When UPPER(TRIM(gen)) IN ('M', 'MALE') Then 'Male'
	 Else 'n/a'
End as gen
From bronze.erp_cust_az12;



Select distinct gen 
from silver.erp_cust_az12;


-- Data Standarization and Consitency 

Select distinct
Case When TRIM(cntry) = 'DE' Then 'Germany'
     When TRIM(cntry) IN ('US', 'USA') Then 'United States'
	 When TRIM(cntry) = '' OR cntry IS NULL Then 'n/a'
	 Else cntry
End as cntry
from bronze.erp_loc_a101
Order by cntry;



Select distinct cntry 
from silver.erp_loc_a101;



-- Check unwanted spaces

Select * from bronze.erp_px_cat_g1v2
Where cat != TRIM(cat) Or subcat != TRIM(subcat) or maintenance != TRIM(maintenance);


Select * from bronze.erp_px_cat_g1v2;

-- Data standarization and consistency 

Select distinct maintenance
from bronze.erp_px_cat_g1v2











