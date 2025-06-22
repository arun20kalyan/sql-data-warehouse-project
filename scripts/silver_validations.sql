--Check for Nulls or Duplicate in Primary Key
-- Result should be empty

select
cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;


--Check for unwanted spaces
-- Result should be empty
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);   

--Check for unwanted spaces
-- Result should be empty
select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname); 



--Data Standardization and consistency
select distinct cst_gndr
from bronze.crm_cust_info;



--Data Standardization and consistency
select distinct cst_marital_status
from bronze.crm_cust_info;



--Check for Nulls or Duplicate in Primary Key
-- Result should be empty

select
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null;


--Check for unwanted spaces
-- Result should be empty
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm); 


---Check for NULLs or Negative Numbers
---Result should be empty
select prd_cost
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null


--Data Standardization and consistency
select distinct prd_line
from bronze.crm_prd_info;


---Check for Invalid Date Orders
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;
