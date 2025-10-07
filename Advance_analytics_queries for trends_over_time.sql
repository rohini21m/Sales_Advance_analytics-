/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'C:\Users\Rohinimunnangi\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'C:\Users\Rohinimunnangi\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'C:\Users\Rohinimunnangi\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

 -------

  select start_date, cost
  from DataWarehouseAnalytics.gold.dim_products
 --- CHANGE OVER TIME TREND 
  -- find the average cost per month  
  select Year(start_date) as start_Year,month(start_date) as start_month, 
  sum(cost) as avg_sales_cost
  from DataWarehouseAnalytics.gold.dim_products
  group by Year(start_date),MONTH(start_date) 
  order by MONTH(start_date)
  
  --find total sales over the year 
  select YEAR(order_date) as Order_Year, 
  SUM(sales_amount) as totalSalesAmount,
  COUNT(distinct customer_key) as total_customers_per_Year -- this shows customers growing /reducing per year with sales
  from DataWarehouseAnalytics.gold.fact_sales
  where order_date is not null
  group by Year(order_date)
  order by Year(order_date) asc  
  
  -- show best performing month according to sale each year 
select 
YEAR(order_date) as Order_Year,
SUM(sales_amount) as totalSalesAmount,
COUNT(distinct customer_key) as total_customers_per_Year, -- this shows customers growing /reducing per year with sal
sum(quantity) as totalquantity
from DataWarehouseAnalytics.gold.fact_sales
where order_date is not null
group by Year(Order_date) 
order by  Year(order_date) asc 

--- best sales performance according to month
select 
FORMAT(order_date,'yyyy-MM') as order_year_month,
SUM(sales_amount) as totalSalesAmount,
COUNT(distinct customer_key) as total_customers_per_Year, -- this shows customers growing /reducing per year with sal
sum(quantity) as totalquantity
from DataWarehouseAnalytics.gold.fact_sales
where order_date is not null
group by FORMAT(order_date,'yyyy-MM')
order by FORMAT(order_date,'yyyy-MM') asc 

---- analysis based on month beginning 
select 
DATETRUNC(MONTH,order_date) as orderyear_month,
SUM(sales_amount) as totalSalesAmount,
COUNT(distinct customer_key) as total_customers_per_Year, -- this shows customers growing /reducing per year with sal
sum(quantity) as totalquantity
from DataWarehouseAnalytics.gold.fact_sales
where order_date is not null
group by DATETRUNC(MONTH,order_date)
order by  DATETRUNC(MONTH,order_date) asc


-- show how many new customers added per month in every year 
select  FORMAT(order_date,'yyyy-MM') as orderyear,
count(distinct customer_key) as new_customers_per_year
from DataWarehouseAnalytics.gold.fact_sales
where FORMAT(order_date,'yyyy-MM') is not null
group by FORMAT(order_date,'yyyy-MM')
order by FORMAT(order_date,'yyyy-MM') asc 

-- cummulative analysis : aggregate the data progressively over the time

--below query gives total_sales per month
select DATETRUNC(month,order_date) as Month_Year,
sum(sales_amount) as total_sales_Each_month
from DataWarehouseAnalytics.gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date)
order by DATETRUNC(month,order_date) asc 

-- Calculate running total_per_sales over the year

select order_year,total_sales_per_month, 
sum(total_sales_per_month) over(order by order_year) as running_total_sales
from (
select DATETRUNC(YEAR,order_date) as order_year, 
sum(sales_amount) as total_sales_per_month
from DataWarehouseAnalytics.gold.fact_sales
where order_date is not null
group by DATETRUNC(YEAR,order_date)
)t 

-- Calculate running total_per_sales over the month

select order_year,total_sales_per_month,
sum(total_sales_per_month) over(order by order_year) as running_total_sales,
avg(avg_price) over( order by order_year) as moving_total
from (
select DATETRUNC(month,order_date) as order_year, 
sum(sales_amount) as total_sales_per_month,
avg(price) as avg_price
from DataWarehouseAnalytics.gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date)
)t 

--Performance Analysis 
-- to track business/sales over time 
--Current Sales VS AvG Sales 
-- write a query to find yearly performance of products by 
--compairing their sales with average sales performance of the year & thier previous year's sales 

with product_yearly_sales as 
(
select  
YEAR(fs.order_date) as order_year,p.product_name,
sum(fs.sales_amount) as product_current_sales
from DataWarehouseAnalytics.gold.dim_products p 
left join DataWarehouseAnalytics.gold.fact_sales fs
on p.product_key=fs.product_key 
where fs.order_date is not null
group by YEAR(fs.order_date),p.product_name
) 
select order_year,product_name,product_current_sales,
LAG(product_current_sales) over (partition by product_name order by order_year ASC) as prev_year_sales,
AVG(product_current_sales) over(partition by product_name) as avg_current_sales,
product_current_sales-avg(product_current_sales) over(partition by product_name) as avg_sales_diff, 
product_current_sales-LAG(product_current_sales) over (partition by product_name order by order_year) as prev_yr_diff,
CASE 
	when product_current_sales-AVG(product_current_sales) over(partition by product_name)>0 then 'Above Avg'
	when product_current_sales-AVG(product_current_sales) over(partition by product_name)<0 then' Below Avg'
	ELSE 'AVG' 
END as avg_change,
CASE 
	when product_current_sales-LAG(product_current_sales) over (partition by product_name order by order_year ASC)>0 then 'Increasing'
	when product_current_sales-LAG(product_current_sales) over (partition by product_name order by order_year ASC)<0 then' Decreasing'
	ELSE 'No Change' 
END as prev_yr_sale_change
from product_yearly_sales
group by order_year,product_name, product_current_sales

-- Part to Whole Proportion 
-- Which categories contribute to overall sales ?

with CategorySales as
(
select p.category as Category, 
sum(s.sales_amount) as total_sales
from  DataWarehouseAnalytics.gold.fact_sales s
left join DataWarehouseAnalytics.gold.dim_products p 
on p.product_key=s.product_key  
where p.category is not null AND s.sales_amount is not null
group by p.category
) 
select Category,total_sales,
sum(total_sales) over() as Overall_Sales,
CONCAT(Round((CAST(total_sales as Float)/SUM(total_sales) over())*100,2),'%') as Sales_Percentage
from CategorySales
order by total_sales desc  -- so bikes are selling the most upto 96%, others are barely performing so its not a good sign.

-- DATA SEGMENTATION 
--Segment products into cost ranges & falls how many products in each category.

with cost_range as (
select product_name, cost, 
CASE 
	when cost between 100 and 500 then '100 to 500'
	when cost<100 then 'below 100'
	when cost between 500 and 1000 then '<1000' 
	else ' Above 1000'
END 
as product_ranges
from DataWarehouseAnalytics.gold.dim_products 
) 
select product_ranges,count(product_name) as product_per_range
from cost_range 
group by product_ranges
order by product_ranges asc 


-- categorise customers based on their spending behaviours 
--VIP :Customers with atleast history of 12 months spending more than $5000
--Regular :Customers with atleast  history of 12 months spending less than $5000
--New :Customers with history of less than  12 months spending less than $1000
--also find no.of customers in each group 

WITH  customer_spending AS 
(
select  c.customer_key, SUM(f.sales_amount) as spending_amount, 
min(f.order_date) as customer_first_order, 
max(f.order_date) as customer_last_order,
DATEDIFF(month,min(f.order_date),max(f.order_date)) as timespan
from DataWarehouseAnalytics.gold.dim_customers c 
left join DataWarehouseAnalytics.gold.fact_sales f
on c.customer_key=f.customer_key  
group by c.customer_key 
)
select customer_groups,count(customer_key) as total_customers
from 
(
select customer_key , spending_amount, timespan,
case 
	when spending_amount >5000 and timespan>=12 then 'VIP'
	when spending_amount <=5000 and timespan>=12 then 'Regular' 
	ELSE 'New'
end as customer_groups 
from customer_spending ) t 
group by customer_groups
order by total_customers desc


-- customer report 
-- this report purpose is to show key customer metrics & behaviours. 
--Highlights : 
--1) gathering fields such as names, ages, transactions, details.
select c.first_name,c.last_name,   
DATEDIFF(Year,c.birthdate,GETDATE()) as customer_age
from DataWarehouseAnalytics.gold.dim_customers c
left join DataWarehouseAnalytics.gold.fact_sales f
on c.customer_key = f.customer_key
--2) customer segmentation metrics: VIP, Regular,New 
--3) Aggregate Customer-level-metrics 
  --a) total orders 
  --b)total sales
  --c)total products
  --d) total quantity purchased 
  --e) lifespan 
--4) Calculates valuable KPI : 
 --a) recency (months since last order)
 --b) average order value 
 -- c) average monthly spend 









