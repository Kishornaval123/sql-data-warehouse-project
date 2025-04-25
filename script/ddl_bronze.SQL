/* 
===================================================================================================
DDL Scripts: Creating Bronze Tables
===================================================================================================
Script Purpose:
        This script creates tables in the 'bronze' schema, dropping existing tables
        if they already exist.
        Run this script to re-define the DDL structure of 'bronze' Tables
====================================================================================================

*/



Use DataWarehouse
-- Create Table CRM Source
-- Create table crm_cust_info


If OBJECT_ID ('bronze.crm_cust_info','U') Is Not Null
       Drop Table bronze.crm_cust_info;

-- Create table crm_cust_info
CREATE TABLE bronze.crm_cust_info (
    csd_id INT,
    csd_key NVARCHAR(50),
    csd_firstname NVARCHAR(50),
    csd_lastname NVARCHAR(50),  -- fixed the missing parenthesis here
    csd_material_status NVARCHAR(50),
    csd_gndr NVARCHAR(50),
    csd_create_date DATE
);


If OBJECT_ID ('bronze.crm_prd_info','U') Is Not Null
       Drop Table bronze.crm_prd_info;

-- Create table crm_prd_info
Create Table bronze.crm_prd_info(

prd_id Int,
prd_key Nvarchar(50),
prd_nm Nvarchar(50),
prd_cost Int,
prd_line Nvarchar(50),
prd_start_dt Datetime,
prd_end_dt Datetime

);



If OBJECT_ID ('bronze.crm_sales_details','U') Is Not Null
       Drop Table bronze.crm_sales_details;

-- Create table crm_sales_details

Create Table bronze.crm_sales_details (

sls_ord_num Nvarchar(50),
sls_prd_key Nvarchar(50),
sls_cust_id Int,
sls_order_dt Int,
sls_ship_dt Int,
sls_due_dt Int,
sls_sales Int,
sls_quantity Int,
sls_price Int

)

-- Create Table of ERP Source


If OBJECT_ID ('bronze.erp_cust_az12','U') Is Not Null
       Drop Table bronze.erp_cust_az12;

-- Create Table of Cust_az12

Create Table bronze.erp_cust_az12(
cid Nvarchar(50),
bdate Date,
gen Nvarchar(50)
);


If OBJECT_ID ('bronze.erp_loc_e101','U') Is Not Null
       Drop Table bronze.erp_loc_e101;

-- Create Table of loc_e101

Create Table bronze.erp_loc_e101(

cid Nvarchar(50),
cntry Nvarchar(50)

);


If OBJECT_ID ('bronze.erp_px_cat_g1v2','U') Is Not Null
       Drop Table bronze.erp_px_cat_g1v2;

-- Create Table of px_cat_g1v2

Create Table bronze.erp_px_cat_g1v2(
id Nvarchar(50),
cat Nvarchar(50),
subcat Nvarchar(50),
maintenance Nvarchar(50)

);






