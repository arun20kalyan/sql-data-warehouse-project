create view gold.dim_customers as
	select 
		row_number() over(order by cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		la.cntry as country,
		ci.cst_marital_status as marital_status,
		ca.bdate as birthdate,
		ci.cst_create_date as create_date,
		case when ci.cst_gndr != 'n/a' then ci.cst_gndr
			else coalesce(ca.gen, 'n/a')
		end as gender 
	from silver.crm_cust_info as ci
	left join silver.erp_cust_az12 as ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 as la
	on ci.cst_key = la.cid



create view gold.dim_products as
select 
	row_number() over(order by pin.prd_start_dt, pin.prd_key) as product_key,
	pin.prd_id as product_id,
	pin.cat_id as category_id,
	pin.prd_key as product_number,
	pin.prd_nm as product_name,
	pin.prd_cost as cost,
	pin.prd_line as product_line,
	pin.prd_start_dt as start_date,
	ec.cat as category,
	ec.subcat as sub_category,
	ec.maintenance
from silver.crm_prd_info as pin
left join silver.erp_px_cat_g1v2 as ec
on pin.cat_id = ec.id
where pin.prd_end_dt is null;  --- filter out all historical data




create view gold.fact_sales as
select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id; 
