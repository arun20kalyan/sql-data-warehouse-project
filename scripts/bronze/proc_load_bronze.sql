

--- Truncate and insert full load method
create or alter procedure bronze.load_bronze as --- creating stored procedure since we have load updated data daily
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time=getdate();
		print 'Loading the bronze layer';
		print '===========================';

		print 'Loading CRM tables';
		print '---------------------------';

		set @start_time =GETDATE();
		print '>>>Truncating bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>>>Inserting into bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\kalya\OneDrive\Desktop\Data Engineering Prep\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time =GETDATE();
		print '>>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '-----------------------------------------------------';


		---select count(*) from bronze.crm_cust_info;

		set @start_time=getdate();
		print '>>>Truncating bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>>>Inserting into bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\kalya\OneDrive\Desktop\Data Engineering Prep\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time =GETDATE();
		print '>>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '-----------------------------------------------------';



		set @start_time=getdate();
		print '>>>Truncating bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
	
		print '>>>Inserting into bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\kalya\OneDrive\Desktop\Data Engineering Prep\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time =GETDATE();
		print '>>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '-----------------------------------------------------';


		print 'Loading ERP tables';
		print '---------------------------';


		set @start_time=getdate();
		print '>>>Truncating bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print '>>>Inserting into bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\kalya\OneDrive\Desktop\Data Engineering Prep\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time =GETDATE();
		print '>>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '-----------------------------------------------------';


		set @start_time=getdate();
		print '>>>Truncating bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
	
		print '>>>Inserting into bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\kalya\OneDrive\Desktop\Data Engineering Prep\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time =GETDATE();
		print '>>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '-----------------------------------------------------';


		set @start_time = getdate();
		print '>>>Truncating bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
	
		print '>>>Inserting into bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\kalya\OneDrive\Desktop\Data Engineering Prep\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time =GETDATE();
		print '>>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '-----------------------------------------------------';


		
		set @batch_end_time =GETDATE();
		print '>>>Load Bacth Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '-----------------------------------------------------';
	end try
	begin catch
		print '=====================================================';
		print 'Error occured during loading bronze layer';
		print 'Error message' + error_message();
		print 'Error number' + cast(error_number() as nvarchar);
		print 'Error state' + cast(error_state() as nvarchar);
		print '=====================================================';
	end catch
end


exec bronze.load_bronze --- code is used to run our stored procedure
