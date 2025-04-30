/*
============================================================================================
DDL Script: Create Gold Views
============================================================================================

Script Purpose:
     This script creates views for the Gold layer in the data warehouse.
     The Gold layer represents the final dimension and fact tables (Star Schema)


     Each view performs transformations and combines data from the Silver layer
     to produce a clean, enriched, and business-ready dataset.


Usage:
  - These views can be queried directly for analytics and reporting.

================================================================================================

*/

-- Create Dimension Customers

If Object_ID ('gold.dim_customers','V')  IS NOT NULL
     DROP View gold.dim_customers;
GO

Create View gold.dim_customers As
Select 
	ROW_NUMBER() Over(Order by cst_id) AS customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status,
	Case When ci.cst_gndr != 'n/a' Then ci.cst_gndr
		 Else Coalesce(ca.gen,'n/a')
	End  gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
from silver.crm_cust_info  ci
Left Join silver.erp_cust_az12 ca
ON      ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 la
ON      ci.cst_key = la.cid;


-- Create Dimension Product 


If Object_ID ('gold.dim_products','V')  IS NOT NULL
     DROP View gold.dim_products;
GO

Create View gold.dim_products As
Select 
    ROW_NUMBER() Over(Order by pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
Left Join silver.erp_px_cat_g1v2 pc
ON    pn.cat_id = pc.id
Where prd_end_dt is null; -- Filter out all historical data



-- Create Fact Sales


If Object_ID ('gold.fact_sales','V')  IS NOT NULL
     DROP View gold.fact_sales;
GO


Create View gold.fact_sales As
Select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as ship_date,
	sd.sls_due_dt  as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details sd
Left Join gold.dim_products pr
ON      sd.sls_prd_key = pr.product_number
Left Join gold.dim_customers cu
ON      sd.sls_cust_id = cu.customer_id





