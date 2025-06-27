
-- SQL Data Analysis Project – Pharmaceutical Sales Dataset
-- Author: Safiya
-- Objective: Analyze pharma sales data, clean it, and extract key business insights using SQL Server
-- using 250000 row from the data

use pharma_data_analysis;

select * from Data_pharma_analysis
-- ------------------------------------------------------
-- Data Cleaning
-- ------------------------------------------------------
-- Check for NULLs

SELECT
  SUM(CASE WHEN Product_Name IS NULL THEN 1 ELSE 0 END) AS null_product_name,
  SUM(CASE WHEN Sales IS NULL THEN 1 ELSE 0 END) AS null_sales,
  SUM(CASE WHEN Quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
  SUM(CASE WHEN name_of_sales_rep IS NULL THEN 1 ELSE 0 END) AS null_sales_rep
FROM Data_pharma_analysis;


-- Delete rows with NULL sales or quantity
delete from Data_pharma_analysis
	where  sales is null or quantity is null 


-- Check for Duplicates
select *, count(*) as duplicate_count
from Data_pharma_analysis
	group by Distributor, Customer_Name, 
	country, channel, sub_channel, 
	Product_Name, product_class, 
	quantity, price, Month, Year, Sales, 
	name_of_sales_rep, manager, sales_team
	having count(*) > 1

--Detect Outliers using IQR

WITH bounds AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sales) OVER () AS Q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sales) OVER () AS Q3
    FROM Data_pharma_analysis
),
limits AS (
    SELECT DISTINCT 
        Q1, 
        Q3, 
        Q3 - Q1 AS IQR,
        Q1 - 1.5 * (Q3 - Q1) AS lower_bound,
        Q3 + 1.5 * (Q3 - Q1) AS upper_bound
    FROM bounds
)
SELECT p.*
FROM Data_pharma_analysis p
CROSS JOIN limits
WHERE sales < lower_bound OR sales > upper_bound
order by quantity desc;
--

-- Create view for cleaned data (e.g., excluding quantities above 60000)
CREATE VIEW pharma AS
SELECT *
FROM Data_pharma_analysis
WHERE quantity < 60000;

-- ------------------------------------------------------
--  KPIs and Insights
-- ------------------------------------------------------
--  Total Sales
select sum(sales)  as Total_sales
from pharma

--Products ------ Top Selling Products

--Highest/Lowest Selling Product

with Total as (
SELECT 
    product_name,
    SUM(Quantity) AS Total_quantity,
    SUM(Sales) AS Total_sales
from pharma
group by product_name
),
Highest as(
	select top 1 product_name, Total_sales , 'Highest' AS Rank_type
	from total
	order by total_sales desc
),
Lowest as (
	select top 1 product_name, Total_sales, 'lowest' AS Rank_type
	from total
	order by total_sales asc
)
select * from Highest
union 
select * from lowest

--Top 5 Products.
select top 5 product_name, sum(sales) as Total_Sales
from pharma
group by product_name
ORDER BY Total_Sales DESC

--Best sales year for each product
SELECT product_name, year, Total_sales
FROM (
    SELECT 
        product_name,
        year,
        SUM(Sales) AS Total_sales,
        DENSE_RANK() OVER (
            PARTITION BY product_name 
            ORDER BY SUM(Sales) DESC
        ) AS DR
    FROM pharma
    GROUP BY product_name, year
) AS ranked_sales
WHERE DR = 1
order by year;

select * from pharma

select year, 
	count(distinct Customer_Name)
from pharma
group by year
order by year

--sales by year
select 
	year, 
	sum(sales) As year_total_sales,
	sum(Quantity) As Total_quantity, 
	count(Customer_Name) as num_customers,
	count(distinct Product_Name) as mum_products_perYear,
	count(distinct Name_of_Sales_Rep) as num_sales_rep
from pharma
group by year
order by year

-- new customers in 2018
select Customer_Name, 
	min(year) As first_year
from pharma
group by customer_name
having min(year)= '2018'

-- ensuring the new customers are in 2018 only 
select first_year as year, 
	count(distinct customer_name) as new_customers
from(
	select Customer_Name, 
		min(year) As first_year
	from pharma
	group by customer_name) as first_appearance
group by first_year
order by first_year

-- new customers exists in any country
select Customer_Name,country, 
	Channel, Sub_channel
from pharma 
group by customer_name, country, Channel, sub_channel
having min(year)= '2018' and max(year)= '2018'
order by customer_name

 -- sales of sales_rep over year
with sales_2017 as
(
select 
	Name_of_Sales_Rep, 
	sum(sales) as sales_2017
from pharma
where year = '2017' 
group by Name_of_Sales_Rep
),
sales_2018 as
(
select 
	Name_of_Sales_Rep, 
	sum(sales) as sales_2018
from pharma
where year = '2018' 
group by Name_of_Sales_Rep
),
sales_2019 as (
select 
	Name_of_Sales_Rep, 
	sum(sales) as sales_2019
from pharma
where year = '2019' 
group by Name_of_Sales_Rep
),
sales_2020 as
(
select 
	Name_of_Sales_Rep, 
	sum(sales) as sales_2020
from pharma
where year = '2020' 
group by Name_of_Sales_Rep
)
select s.Name_of_Sales_Rep, sales_2017, sales_2018, sales_2019, sales_2020
from sales_2018 s join sales_2019 e
on s.Name_of_Sales_Rep = e.Name_of_Sales_Rep
join sales_2017 a
on e.Name_of_Sales_Rep = a.Name_of_Sales_Rep
join sales_2020 b
on a.Name_of_Sales_Rep = b.Name_of_Sales_Rep


--sales by month
select month, sum(sales) month_total_sales
from pharma
group by month
order by month_total_sales desc

select year, month, sum(sales) 
from pharma
group by month, year
order by year

-- Product Sales Trend & Growth by year and month

with sales as (
select 
	month, 
	year, 
	sum(sales) as date_sales, 
	lag(sum(sales)) over (partition by month order by year) as prev_YearSales_InMonth
from pharma
group by month, year
),
Growth as(
select * , 
	date_sales - prev_YearSales_InMonth as sales_diff,
	round(cast(date_sales - prev_YearSales_InMonth as float) / nullif(prev_YearSales_InMonth,0),2)
	as relative_growth
from sales
)
select * , 
	(case when relative_growth > .6 then 'excellent' 
	 when relative_growth > .3 then 'good' 
	 when relative_growth >= .09 then 'Med' 
	 when relative_growth < .09 then 'bad'
	else 'No data ' 
	end) as Status_growth
from growth 
-- Product Sales Trend & Growth

WITH sales_data AS (
  SELECT 
      product_name,
      year,
      SUM(sales) AS total_sales
  FROM pharma
  GROUP BY product_name, year
),
with_growth AS (
  SELECT 
      product_name,
      year,
      total_sales,
      LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year) AS previous_year_sales,
      total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year) AS sales_difference,
      round(CAST(total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year) AS FLOAT) 
        / NULLIF(LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year), 0),2) AS relative_growth
  FROM sales_data
)
SELECT *,
       DENSE_RANK() OVER (PARTITION BY product_name ORDER BY relative_growth DESC) AS growth_rank
FROM with_growth
ORDER BY product_name, year;

-- Cumulative Sales Per Product 
select product_name, 
year, 
sum(sales) as yearly_sales,
sum(sum(sales)) over (partition by product_name order by year) as cumulative_sales
from pharma
group by product_name, year
order by product_name, year

-- total  sales within each country and country sales percent
with sales as
(
select sum(sales) as total_sales
from pharma
),
sales_country as (
select country, count(distinct product_name) as num_product, sum(sales) as country_sales
from pharma
group by country
)
select country, country_sales, concat( country_sales / total_sales, '%') as sales_percent
from sales, sales_country

--Channel & Sub-channel Analysis

-- num of products in each channel
Select channel, 
count(distinct sub_channel) as num_sub_Channel, 
count(distinct product_name) as num_products
from pharma
group by channel

-- Products in Pharmacy but Not in Hospital
--all products in pharmacy are in hospital
select  distinct product_name
from pharma
where channel= 'pharmacy' 
except
select distinct product_name
from pharma
where channel = 'hospital'

-- Total Quantity per Channel
select channel, 
round(sum(quantity),0) as total_quantity
from pharma 
group by channel

--Total Sales & Sales Percentage per Channel
select channel, 
sum(sales) as total_sales,  
sum(sales)*100 / (select sum(sales) from pharma)AS sales_percentage
from pharma
group by channel

-- Cumulative Sales per Channel per Year
select channel, year, 
sum(sales) as yearly_sales ,
sum(sum(sales)) over (partition by channel order by year asc) as cumilative_sales
from pharma
group by channel, year
order by channel, year

-- Cumulative Sales per Sub-channel per Year
select sub_channel, year, sum(sales) as yearly_sales ,sum(sum(sales)) over (partition by sub_channel order by year asc) as cumilative_sales
from pharma
group by sub_channel, year
order by sub_channel, year

-- Total sales per subchannel and per country
Select sub_channel, country, sum(sales) as Total_sales
from pharma
group by sub_channel, country
order by Total_sales desc

--Total Sales and Quantity per Sub-channel
select sub_channel, sum(quantity) as total_quantity, sum(sales) as total_sales
from pharma
group by sub_channel;

 -- Product Variety per Channel Over Time

 select channel, 
 year, 
 count(distinct product_name) AS num_products
 from pharma
 group by channel, year

-- Team Performance Analysis

-- Number of Teams & Total Sales per Country

select country, 
count(distinct sales_team) as num_teams, 
sum(sales) as total_sales
from pharma
group by country


----------------------------
-- Total Sales and Quantity per Team

select sales_team, 
sum(quantity) as total_quantity, 
sum(sales) as total_sales
from pharma
group by sales_team
order by total_sales desc

 --- Top Sales Team Overall
select top 1 sales_team, 
sum(quantity) as total_quantity, 
sum(sales) as total_sales
from pharma
group by sales_team
order by total_sales desc

-- Best Sales Team per Year
WITH ranked_teams AS (
  SELECT 
    sales_team, 
    year, 
    SUM(quantity) AS total_quantity, 
    SUM(sales) AS total_sales,
    DENSE_RANK() OVER (PARTITION BY year ORDER BY SUM(sales) DESC) AS dr
  FROM pharma
  GROUP BY sales_team, year
)
SELECT  sales_team, 
		year,
		total_quantity,
		total_sales
FROM ranked_teams
WHERE dr = 1
ORDER BY year, total_sales DESC;

-- Sales Comparison: Germany vs. Poland for Delta Team

select sales_team, country, sum(sales) as total_sales
from pharma
where sales_team = 'delta'
group by sales_team, country

--Sales Comparison: All Teams in Germany vs. Poland
select sales_team, country, sum(sales) as total_sales
from pharma
group by sales_team, country
order by sales_team desc

--Top-Selling Product per Team per Country
select * from(
select sales_team, product_name, country, sum(sales) as T_sales, 
dense_rank() over (partition by sales_team, country order by sum(sales) desc) as DR
from pharma
group by sales_team, product_name, country)t
where dr = 1

--  Average Order & Rep Performance per Team
select *, 
	round(CAST(T_sales AS FLOAT) / NULLIF(num_orders, 0),2) AS AVG_order, 
	round(CAST(T_sales AS FLOAT) / NULLIF(num_sales_rep, 0),2) AS AVG_per_rep
from(
	select sales_team, 
		count(distinct name_of_sales_rep) as num_sales_rep, 
		count(sales) as num_orders, 
		sum(sales) as T_sales
	from pharma
	group by sales_team)t

-- Sales Rep Performance

-- Top Sales Rep Overall
select * from (
select name_of_sales_rep, sales_team, sum(sales) as total_rep_sales, 
rank() over (order by sum(sales) desc) ranking 
from pharma
group by name_of_sales_rep, sales_team)t
where ranking = 1

--  Top Sales Rep in Each Team
select * 
from (
select name_of_sales_rep, sales_team, sum(sales) as total_rep_sales,
dense_rank() over (partition by sales_team order by sum(sales) desc) as ranking
from pharma
group by name_of_sales_rep, sales_team)t
where ranking = 1
order by total_rep_sales desc

--  Top Sales Rep in Each Channel
select name_of_sales_rep, channel,num_orders, total_rep_sales
from (
select name_of_sales_rep, channel, 
count(sales) as num_orders, 
sum(sales)  as total_rep_sales,
rank() over (partition by channel order by sum(sales) desc) as ranking
from pharma
group by name_of_sales_rep, channel
)t
where ranking = 1

-- Sales Growth Per Rep Over the Years
with Total_sales as(
select name_of_sales_rep, year, sum(sales) as sales_year_rep
from pharma 
group by name_of_sales_rep, year
)
select *, 
lag(sales_year_rep) over (partition by name_of_sales_rep order by year) prev_year_sales,
sales_year_rep - lag(sales_year_rep) over (partition by name_of_sales_rep order by year) as sales_diff,
round(cast(sales_year_rep - lag(sales_year_rep) over (partition by name_of_sales_rep order by year) as float)/
nullif(lag(sales_year_rep) over (partition by name_of_sales_rep order by year), 0),2) as relatvie_growth
from Total_sales
ORDER BY name_of_sales_rep, year

 -- Total Sales per Manager by Team

select manager, sales_team, sum(sales) as mang_sales
from pharma
group by manager, sales_team
order by mang_Sales desc



--Product Category Analysis

--Total Sales per Product Class
select product_class, sum(sales) as T_class_sales
from pharma
group by product_class
order by T_class_sales desc

-- 
--Total Sales per Product Class per Year
select product_class,year, sum(sales) as T_class_sales
from pharma
group by product_class, year
order by product_class, year 


