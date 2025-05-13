/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


Create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time=getdate()
		print'=================================================';
		print'Loadind Silver layer';
		print'=================================================';
		print'';

		print'=================================================';
		print'Loadind CRM Table';
		print'=================================================';
		print'';
		print'=================================================';
		print'Truncate and Load into:silver.crm_cust_info';
		print'=================================================';

		set @start_time=getdate()
		if OBJECT_ID('silver.crm_cust_info','u') is not null
			truncate table silver.crm_cust_info

		insert into silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date)
		select 
			cst_id,
			cst_key,
			Trim (cst_firstname) as cst_firstname,
			Trim (cst_lastname) as cst_lastname,
			case when Upper(Trim(cst_marital_status))= 'S'  then 'Single' 
				when Upper(Trim(cst_marital_status))= 'M' then 'Married'
				else 'n/a'
			end cst_marital_status,
			case when Upper(Trim(cst_gndr))= 'F'  then 'Female' 
				when Upper(Trim(cst_gndr))= 'M' then 'Male'
				else 'n/a'
			end cst_gndr,
			cst_create_date
		from
			(select *,
			rank()over(partition by cst_id order by cst_create_date desc) as 'rank'
			from bronze.crm_cust_info
			where cst_id is not null)t
			where rank=1

		Set @end_time=getdate();
		print '>>>load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'Second';
		print'================================================================';


		print'=================================================';
		print'Truncate and Load into:silver.crm_prd_info';
		print'=================================================';
		set @start_time=getdate()
		if OBJECT_ID('silver.crm_prd_info','u') is not null
			Truncate Table silver.crm_prd_info

		insert into silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt)


		select 
			prd_id,
			replace(substring(prd_key,1,5),'-','_' )as cat_id,
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			prd_nm,
			isnull(prd_cost,0) as prd_cost ,
			case when UPPER(Trim(prd_line))='M' then 'Mountain'
				when UPPER(Trim(prd_line))='R' then 'Road'
				when UPPER(Trim(prd_line))='S' then 'Other Sales'
				when UPPER(Trim(prd_line))='T' then 'Touring'
				else 'n/a'
				end prd_line,
			cast(prd_start_dt as date)prd_start_dt,
			cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt )-1 as date)  prd_end_dt
		from bronze.crm_prd_info

		Set @end_time=getdate();
		print '>>>load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'Second';
		print'================================================================';



		print'=================================================';
		print'Truncate and Load into:silver.crm_sales_details';
		print'=================================================';
		set @start_time=getdate()
		if OBJECT_ID('silver.crm_sales_details','u') is not null
			truncate table silver.crm_sales_details

		insert into silver.crm_sales_details(
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

		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when (len(sls_order_dt)!=8) or (sls_order_dt=0) then null
				else cast(cast(sls_order_dt as char) as date)
				end as sls_order_dt,
			case when (len(sls_ship_dt)!=8) or (sls_ship_dt=0) then null
				else cast(cast(sls_ship_dt as char) as date)
				end as sls_ship_dt,
			case when (len(sls_due_dt)!=8) or (sls_due_dt=0) then null
				else cast(cast(sls_due_dt as char) as date)
				end as sls_due_dt,
			 case when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity*abs(sls_price) 
				then sls_quantity*abs(sls_price)
				else sls_sales
				end sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price<=0
				then sls_sales/nullif(sls_quantity,0)
				else sls_price
				end as sls_price
		from bronze.crm_sales_details

		Set @end_time=getdate();
		print '>>>load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'Second';
		print'================================================================';

		print'=================================================';
		print'Loading ERP Table';
		print'=================================================';


		print'=================================================';
		print'Truncate and Load into:silver.erp_cust_az12';
		print'=================================================';
		set @start_time=getdate()
		if OBJECT_ID('silver.erp_cust_az12','u') is not null
			truncate table silver.erp_cust_az12

		insert into silver.erp_cust_az12(
				cid,
				bdate,
				gen)

		select 
		case when cid like 'NAS%' then substring(cid,4,len(cid)) 
			else cid
			end as cid,
		case when bdate > getdate() then null 
			else bdate
			end as bdate,
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			when upper(trim(gen)) in ('M','MALE') then 'Male'
			else 'n/a'
			end gen
		from bronze.erp_cust_az12

		Set @end_time=getdate();
		print '>>>load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'Second';
		print'================================================================';


		print'=================================================';
		print'Truncate and Load into:silver.erp_loc_a101';
		print'=================================================';
		set @start_time=getdate()
		if OBJECT_ID('silver.erp_loc_a101','u') is not null
			truncate table silver.erp_loc_a101 

		insert into  silver.erp_loc_a101(
				cid,
				cntry
		)
		select 
		replace (cid,'-','') as cid,
		case when Trim(cntry) ='DE' THEN 'Germany'
			when Trim(cntry)in ('US','USA') Then 'United States'
			when Trim(cntry)= '' or cntry is null then 'n/a'
			else Trim(cntry)
			end cntry
		from bronze.erp_loc_a101

		Set @end_time=getdate();
		print '>>>load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'Second';
		print'================================================================';

		print''
		print'=================================================';
		print'Truncate and Load into:silver.erp_px_cat_g1v2';
		print'=================================================';
		
		set @start_time=getdate()
		if OBJECT_ID('silver.erp_px_cat_g1v2','u') is not null
			truncate table silver.erp_px_cat_g1v2
		 insert into silver.erp_px_cat_g1v2
		 (
				id,
				cat,
				subcat,
				maintenance
				)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2

		Set @end_time=getdate();
		print '>>>load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'Second';
		print'================================================================';

		print''
		set @batch_end_time=getdate()
		print'>>> Total load duration:'+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+'Second';
	end try
	begin catch
		print '===========================================';
		print 'ERROR OCCURED DURING LOADING BRONGE LAYER';
		PRINT 'Error message'+ ERROR_MESSAGE();
		PRINT 'Error message'+ cast(error_number() as nvarchar);
		print 'Error message'+ cast(error_state() as nvarchar);
		print '============================================'
	end catch
End




