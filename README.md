# **AdventureWorks Business Activities Analysis with SQL on BigQuery**

👤 Author: Nguyen Luu Bao Duy

🛠️ Tool Used: SQL

# 📑 Table of Contents
- [📌 Project Overview](#---project-overview)

- [🗄️ Dataset](#----dataset)

- [🧩 Analysis Approach](#---analysis-approach)

- [💡 Insights and Recommendations](#---insights-and-recommendations)

# 📌 Project Overview

## 🎯 Project Objectives

This project uses the AdventureWorks dataset available on BigQuery to explore the activities of a simulated multinational bicycle manufacturer and distributor through SQL queries. Its objective is to showcase SQL analytics skills by analyzing key business factors - such as sales performance, year-over-year growth, territory rankings, discount costs, customer retention, stock level trends, and pending orders - in order to extract meaningful insights that reflect real-world business challenges.

## ❓ Core Questions

The analysis focuses on key questions:

- **Sales performance** – How do sales, order volumes, and growth trends vary by product subcategory and territory?
- **Pricing & promotions** – What is the financial impact of seasonal discounts on different product subcategories?
- **Customer dynamics** – What is the retention rate of customers over time, and how do shipment outcomes influence it?
- **Inventory & operations** – How do stock levels evolve month by month, what is the ratio of stock to sales, and how does it affect efficiency?
- **Order management** – How many orders remain pending, and what is their associated value?

## 👥 Target audience

- Sales & Marketing Team
- Operations Team
- Analytics & Data Team
- Business Strategy & Management Team
- Finance Team

# 🗄️ Dataset

## 📂 Source

- Dataset Name: AdventureWorks2019
- Provided by: Microsoft
- Dataset ID: `adventureworks2019`

## 📖 Description

- Scenario: **Adventure Works Cycles** is a fictional global manufacturing company that produces and distributes bicycles and related accessories to commercial markets.
- The dataset includes a variety of tables, typically organized into categories such as:
    - **Sales**: Information about sales orders, products, and customer details.
    - **Production**: Data on manufacturing processes, inventory, and product specifications.
    - **Human Resources**: Employee details, departments, and job roles.
    - **Purchasing**: Vendor information and purchase orders.
- Dataset Dictionary: [View more](https://drive.google.com/file/d/1bwwsS3cRJYOg1cvNppc1K_8dQLELN16T/view)

## ⏳ Time Frame

- The dataset includes records spanning from 2011 to 2014.
- The analysis is conducted for the same period.

# 🧩 Analysis Approach

## 🔍 Sales values, order quantities, and item quantities by subcategory in the last 12 months

This analysis aims to evaluate overall sales performance at the product subcategory level. The result reveals sales trends over the month and the contribution of each product sub-group to revenue and demand in the recent year.

📜 **Query**

```sql
SELECT
  FORMAT_DATE('%Y-%m', DATE(sod.ModifiedDate)) AS period
  , sub.Name AS subcat
  , SUM(sod.OrderQty) AS item_qty
  , SUM(sod.LineTotal) AS total_sales
  , COUNT(DISTINCT sod.SalesOrderID) AS order_count
FROM `adventureworks2019.Sales.SalesOrderDetail` AS sod
LEFT JOIN `adventureworks2019.Production.Product` AS pro
  ON sod.ProductID = pro.ProductID
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS sub
  ON CAST(pro.ProductSubcategoryID AS INT64) = sub.ProductSubcategoryID
WHERE DATE(sod.ModifiedDate) >= DATE_SUB((SELECT DATE(MAX(ModifiedDate)) 
                                          FROM `adventureworks2019.Sales.SalesOrderDetail`), INTERVAL 12 MONTH)
GROUP BY period, subcat
ORDER BY subcat, period DESC;
```

✔️ **Result**

<img width="2000" height="839" alt="Image" src="https://github.com/user-attachments/assets/6bef2261-eb22-4a2b-90b1-481dbe71448f" />

## 🔍 Year-over-year growth rate by subcategory

This analysis aims to measure year-over-year growth dynamics across product subcategories. The result highlights the areas of the business that are expanding fastest, and pinpoints where investment opportunities may lie.

📜 **Query**

```sql
WITH
curr AS(
  SELECT
    sub.Name AS subcat
    ,EXTRACT(YEAR FROM DATE(sod.ModifiedDate)) AS year
    , SUM(sod.OrderQty) AS cur_year_qty
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS sod
  LEFT JOIN `adventureworks2019.Production.Product` AS pro
    ON sod.ProductID = pro.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS sub
    ON CAST(pro.ProductSubcategoryID AS INT64) = sub.ProductSubcategoryID
  GROUP BY subcat, year
  ORDER BY subcat, year
)

, prev AS(
  SELECT
    subcat
    , year
    , cur_year_qty
    , LAG(cur_year_qty) OVER(PARTITION BY subcat ORDER BY year) AS prev_year_qty
  FROM curr
)

SELECT
  subcat
  , year
  , cur_year_qty
  , prev_year_qty
  , ROUND(100*(cur_year_qty/prev_year_qty - 1),2) AS yoy_rate
FROM prev
ORDER BY yoy_rate DESC;
```

✔️ **Result**

<img width="2000" height="841" alt="Image" src="https://github.com/user-attachments/assets/888d1edc-4070-4773-94d6-c67c4561e6e6" />

## 🔍 Top 3 territories by yearly order quantities

This analysis aims to measure the geographical sales performance of territories based on order volume. The result not only pinpoints the top three territories each year but also provides insights into their competitive standing over time.

📜 **Query**

```sql
WITH
order_qty_terr AS(
  SELECT  
    FORMAT_DATE('%Y',DATE(sod.ModifiedDate)) AS year
    , soh.TerritoryID AS territory_id
    , SUM(sod.OrderQty) total_order_qty
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS sod
  LEFT JOIN `adventureworks2019.Sales.SalesOrderHeader` AS soh
    ON sod.SalesOrderID = soh.SalesOrderID
  GROUP BY year, territory_id
)

, ranking AS(
  SELECT
    year
    , territory_id
    , total_order_qty
    , DENSE_RANK() OVER(PARTITION BY year ORDER BY total_order_qty DESC) AS rnk
  FROM order_qty_terr
)

SELECT
  year
    , territory_id
    , total_order_qty
    , rnk
FROM ranking
WHERE rnk <= 3
ORDER BY year;
```


✔️ **Result**

<img width="2000" height="840" alt="Image" src="https://github.com/user-attachments/assets/9217cc4d-ac78-4931-a1c0-ded1e8168125" />

## 🔍 Total seasonal discount cost by subcategory

This analysis aims to evaluate the effectiveness and cost of seasonal promotions. The result quantifies the actual cost for the promotions, how much revenue is reduced by discounts, and identifies which subcategories bear the largest costs.

📜 **Query**

```sql
SELECT
  FORMAT_DATE('%Y', DATE(sod.ModifiedDate)) AS year
  , sub.Name AS subcat
  , SUM(so.DiscountPct*sod.UnitPrice*sod.OrderQty) AS total_dis_cost
FROM `adventureworks2019.Sales.SalesOrderDetail` AS sod
LEFT JOIN `adventureworks2019.Production.Product` AS pro
  ON sod.ProductID = pro.ProductID
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS sub
  ON CAST(pro.ProductSubcategoryID AS INT64) = sub.ProductSubcategoryID
LEFT JOIN `adventureworks2019.Sales.SpecialOffer` AS so
  ON sod.SpecialOfferID = so.SpecialOfferID
WHERE LOWER(so.Type) LIKE '%seasonal discount%'
GROUP BY subcat, year;
```


✔️ **Result**

<img width="2000" height="360" alt="Image" src="https://github.com/user-attachments/assets/853922c5-60e8-459e-8af8-a4ad159bbfaa" />

## 🔍 Customer retention rate in 2014 (cohort analysis on successful shipments)

This analysis aims to track how effectively the company retains customers over time. The result shows customer loyalty and identifies drop-off rates across customer cohorts in 2014.

📜 **Query**

```sql
WITH 
order_info AS(
  SELECT
    EXTRACT(MONTH FROM ModifiedDate) AS order_month
    , CustomerID
  FROM `adventureworks2019.Sales.SalesOrderHeader`
  WHERE Status = 5 AND EXTRACT(YEAR FROM ModifiedDate) = 2014
  GROUP BY order_month, CustomerID
)

, row_num AS(
  SELECT
    order_month
    , CustomerID
    , ROW_NUMBER() OVER(PARTITION BY CustomerID ORDER BY order_month) AS rn
  FROM order_info
)

, first_order_info AS(
  SELECT
    order_month AS join_month
    , CustomerID
  FROM row_num
  WHERE rn = 1
)

SELECT
  join_month
  , CONCAT(order_month - join_month,' month(s)') AS after
  , COUNT(o.CustomerID) AS num_customer
FROM order_info AS o
LEFT JOIN first_order_info AS f
  ON o.CustomerID = f.CustomerID
GROUP BY join_month, after
ORDER BY join_month, after;
```

✔️ **Result**

<img width="2000" height="837" alt="Image" src="https://github.com/user-attachments/assets/e67a678d-426b-4b74-84bf-032053f3eb3b" />

## 🔍 Stock level trend and MoM percentage change in 2011

This analysis aims to monitor inventory levels and month-over-month changes. The result helps to detect supply chain fluctuations and assess whether stock management aligns with demand.

📜 **Query**

```sql
WITH
cur AS(
  SELECT
    p.Name AS product_name
    , EXTRACT(MONTH FROM w.ModifiedDate) AS month
    , EXTRACT(YEAR FROM w.ModifiedDate) AS year
    , SUM(StockedQty) AS cur_stock
  FROM `adventureworks2019.Production.Product` AS p
  LEFT JOIN `adventureworks2019.Production.WorkOrder` AS w
    ON p.ProductID = w.ProductID
  WHERE EXTRACT(YEAR FROM w.ModifiedDate) = 2011
  GROUP BY product_name, month, year
)

, prev AS(
  SELECT
    product_name
    , month
    , year
    , cur_stock
    , LAG(cur_stock) OVER(PARTITION BY product_name ORDER BY month) AS prev_stock
  FROM cur
)

SELECT
  product_name
  , month
  , year
  , cur_stock
  , prev_stock
  , ROUND(IFNULL(100.0*(cur_stock/prev_stock - 1),0),1) AS  mom_rate
FROM prev
ORDER BY product_name, month DESC;
```

✔️ **Result**

<img width="2000" height="840" alt="Image" src="https://github.com/user-attachments/assets/b892efdc-6565-407b-b80e-6cd17155a4bb" />

## 🔍 Stock-to-sales ratio by product and month in 2011

This analysis aims to evaluate the balance between inventory and sales. The result helps to identify products that are overstocked or understocked relative to sales volume, providing insights for predictive supply and demand management.

📜 **Query**

```sql
WITH
sales AS(
  SELECT
    EXTRACT(MONTH FROM sod.ModifiedDate) AS month
    , EXTRACT(YEAR FROM sod.ModifiedDate) AS year
    , sod.ProductID AS product_id
    , p.Name AS product_name
    , SUM(sod.OrderQty) AS sales_qty
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS sod
  LEFT JOIN `adventureworks2019.Production.Product` AS p
    ON sod.ProductID = p.ProductID
  WHERE EXTRACT(YEAR FROM sod.ModifiedDate) = 2011
  GROUP BY product_id, product_name, month, year
)

, stock AS(
  SELECT
    EXTRACT(MONTH FROM wo.ModifiedDate) AS month
    , EXTRACT(YEAR FROM wo.ModifiedDate) AS year
    , wo.ProductID AS product_id
    , SUM(StockedQty) AS stock_qty
  FROM `adventureworks2019.Production.WorkOrder` AS wo
  WHERE EXTRACT(YEAR FROM wo.ModifiedDate) = 2011
  GROUP BY product_id, month, year
)

SELECT
  sales.month
  , sales.year 
  , sales.product_id
  , product_name
  , sales_qty
  , stock_qty
  , ROUND(COALESCE(stock_qty/sales_qty,0),1) AS ratio
FROM sales 
LEFT JOIN stock
  ON sales.product_id = stock.product_id
  AND sales.month = stock.month
ORDER BY month DESC, ratio DESC;
```

✔️ **Result**

<img width="2000" height="837" alt="Image" src="https://github.com/user-attachments/assets/6740883b-a5f9-4cab-85c7-1f98c44a3df2" />

## 🔍 Number and value of pending orders in 2014

This analysis aims to assess operational efficiency in order processing. The result helps identify bottlenecks caused by unfulfilled orders and estimate their impact on revenue.

📜 **Query**

```sql
SELECT
  EXTRACT(YEAR FROM ModifiedDate) AS year
  , Status
  , COUNT(DISTINCT SalesOrderID) AS order_count
  , SUM(TotalDue) AS total_value
FROM `adventureworks2019.Sales.SalesOrderHeader`
WHERE EXTRACT(YEAR FROM ModifiedDate) = 2014 AND Status = 1
GROUP BY year, Status
```

✔️ **Result**

<img width="2000" height="340" alt="Image" src="https://github.com/user-attachments/assets/283dab02-68ee-4475-b2de-6cda53803d5c" />

# 💡 Insights and Recommendations
