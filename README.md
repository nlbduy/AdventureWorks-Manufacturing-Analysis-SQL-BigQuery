# 🚲**AdventureWorks Manufacturing Business Activities Analysis with SQL on BigQuery**

<img width="2000" height="1600" alt="Image" src="https://github.com/user-attachments/assets/0d12a3f4-ced0-401d-aa69-71e81da96504" />

👤 Author: Nguyen Luu Bao Duy

🛠️ Tool Used: SQL

# 📑 Table of Contents
- [📌 Project Overview](#---project-overview)

- [🗄️ Dataset](#----dataset)

- [🧩 Analysis Approach](#---analysis-approach)

- [💡 Insights and Recommendations](#---insights-and-recommendations)

# 📌 Project Overview

## 🎯 Project Objectives

This project uses the **AdventureWorks** dataset available on BigQuery to explore the activities of a simulated multinational bicycle manufacturer and distributor through SQL queries. Its objective is to showcase SQL analytics skills by analyzing key business factors - such as **sales performance**, **year-over-year growth**, **territory rankings**, **discount costs**, **customer retention**, **stock level trends**, and **pending orders** - in order to extract meaningful insights that reflect real-world business challenges.

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

- Dataset Name: AdventureWorks2019

- The dataset includes a variety of tables, typically organized into categories such as:
    - **Sales**: Information about sales orders, products, and customer details.
    - **Production**: Data on manufacturing processes, inventory, and product specifications.
    - **Human Resources**: Employee details, departments, and job roles.
    - **Purchasing**: Vendor information and purchase orders.
- Dataset Dictionary: [View more](https://drive.google.com/file/d/1bwwsS3cRJYOg1cvNppc1K_8dQLELN16T/view)
- The dataset includes records spanning from 2011 to 2014.

# 🧩 Analysis Approach

## 🔍 Sales values, order quantities, and item quantities by subcategory in the last 12 months

This analysis aims to evaluate **overall sales performance** at the product subcategory level. The result reveals sales trends over the month and the contribution of each product sub-group to revenue and demand in the recent year.

📜 **Query**

```sql
WITH l12m_performance AS (
  SELECT
    FORMAT_DATE('%Y-%m', DATE(sod.ModifiedDate)) AS period
    , sub.Name AS subcat
    , ROUND(SUM(sod.LineTotal),0) AS monthly_sales 
    -- total sales value of the subcategory for this month
    , SUM(sod.OrderQty) AS item_qty 
    -- total items sold in the subcategory for this month
    , COUNT(DISTINCT sod.SalesOrderID) AS order_count 
    -- number of orders containing this subcategory
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS sod
  LEFT JOIN `adventureworks2019.Production.Product` AS pro
    ON sod.ProductID = pro.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS sub
    ON CAST(pro.ProductSubcategoryID AS INT64) = sub.ProductSubcategoryID
  WHERE DATE(sod.ModifiedDate) >= DATE_SUB(
          (SELECT DATE(MAX(ModifiedDate)) 
           FROM `adventureworks2019.Sales.SalesOrderDetail`), INTERVAL 12 MONTH)
  GROUP BY period, subcat
  ORDER BY subcat, period DESC
)

SELECT
  period
  , subcat
  , monthly_sales
  , item_qty
  , order_count
  , ROUND(SUM(monthly_sales) OVER(PARTITION BY subcat),0) AS total_sales 
  -- total sales value in the last 12 months for this subcategory
  , SUM(item_qty) OVER(PARTITION BY subcat) AS total_item_qty
  -- total item sold in the last 12 months for this subcategory
  , SUM(order_count) OVER(PARTITION BY subcat) AS total_order_count
  -- total orders in the last 12 months for this subcategory
  , COUNT(subcat) OVER(PARTITION BY subcat) AS on_sales_months 
  -- number of months in which the subcategory recorded sales
FROM l12m_performance
ORDER BY total_sales DESC, on_sales_months DESC;

```

✔️ **Result**

<img width="2000" height="830" alt="Image" src="https://github.com/user-attachments/assets/e3664349-a94f-41af-8ace-afd4bb70fc9f" />

The results highlight **Mountain Bikes**, **Road Bikes**, and **Touring Bikes** as the top three subcategories, consistently generating revenue across all 12 months. Together, they account for more than **80%** of total sales performance, establishing them as the primary revenue drivers of the business.

## 🔍 Year-over-year growth rate by subcategory

This analysis evaluates **year-over-year growth** in **item quantity ordered** across product subcategories. It helps identify which areas of the business are expanding most rapidly and where potential investment opportunities may exist.

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
ORDER BY subcat, year, yoy_rate DESC;
```

✔️ **Result**

<img width="2000" height="842" alt="Image" src="https://github.com/user-attachments/assets/d21e3c3b-57d7-4300-aa86-444b31827971" />

The year-over-year growth rate in item quantity ordered has shown a consistent decline over time. Notably, all subcategories recorded **negative growth in 2014** - including key revenue drivers such as Mountain Bikes, Road Bikes, and Touring Bikes, which had historically delivered high sales volumes and strong performance. This downturn signals potential structural challenges in the market and highlights the need for further investigation into demand shifts and customer preferences.

## 🔍 Top 3 territories by yearly item quantity ordered

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

The analysis reveals a clear and stable ranking among the top three territories (4, 6, and 1), with no shifts in their relative positions over the observed period. **Territory 4 consistently stands out as the market leader**, maintaining the top spot with a significant performance gap compared to the others. **Territory 6 remains firmly in second place**, demonstrating steady performance but without closing the gap to Territory 4. Meanwhile, **Territory 1 consistently trails behind**, securing third place but with a noticeable distance from the top two leaders. This pattern highlights a **highly concentrated market structure**, where **Territories 4 and 6 dominate**, while **Territory 1 plays more of a supporting role** rather than a true growth driver.

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

**Helmets was the only subcategory that recorded seasonal discounts**, which doubled in cost from 2012 to 2013. Given that Helmets is not a key revenue driver, this pattern suggests it may have been used as a test item for discount campaigns. However, as all subcategories showed declining year-over-year growth and turning negative in 2014, the discount program for Helmets was likely discontinued in response to weak demand and limited effectiveness.

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

<img width="2000" height="1423" alt="Image" src="https://github.com/user-attachments/assets/e9edf357-8367-4b45-bd71-5b70d937e249" />

The Retention Rate was recorded at a very low level, with **most customers churning after the first month**, except for an unusual increase in Month 3 for Cohorts 1 and 2, which may have been driven by a promotional program or targeted campaign. However, given the specific nature of the business - bicycles and accessories - **this pattern is reasonable**. Bicycles have a long product lifecycle, meaning customers rarely make repeat purchases in the short term. A low Retention Rate is therefore expected, as returning customers are more likely to purchase accessories with shorter lifecycles or complementary items following their initial bicycle purchase.

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

The results reveal sharp month-to-month swings in stock levels. **Negative rates** suggest demand exceeded replenishment, creating **stockout risks**, while **high positives indicate potential overstocking and higher holding costs**. These fluctuations signal **gaps in inventory planning** and highlight the need for better demand forecasting and more balanced replenishment strategies.

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

Products with high stock-to-sales ratios (SSR) such as **HL Mountain Frame - Black (48, 42)**, **HL Mountain Frame - Silver (38)**, **LL Road Frame - Black (58)**, and **HL Mountain Frame - Black (38)** indicate significant **overstocking** relative to sales volume. In particular, the **HL Mountain Frame group (Black and Silver variants)** requires close monitoring, as these items show the most pronounced supply-demand imbalance, raising the risk of prolonged inventory holding, increased storage costs, and potential markdowns.

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

The analysis shows that **there were no pending orders** recorded in 2014. This indicates that the company maintained strong operational efficiency in order processing, with no backlogs affecting customer experience or revenue realization.

# 💡 Insights and Recommendations

## 🌟 Key Insights

**Revenue Concentration & Market Dependence**
- Revenue is heavily concentrated in three subcategories - **Mountain Bikes**, **Road Bikes**, and **Touring Bikes** - which consistently contribute more than 80% of total sales.
- Likewise, **Territories 4 and 6** dominate geographical performance, with Territory 4 standing out as the clear leader.
- This pattern reflects a strong reliance on a few products and markets, creating **concentration risks** in the event of demand fluctuations.

**Demand Weakening & Growth Challenges**
- Year-over-year growth in item quantities has **steadily declined**, turning **negative** across all subcategories in **2014**, including key revenue drivers.
- **Seasonal discount was discontinued** due to weak demand and limited effectiveness possibilities.
- These signals suggest a **demand slowdown** or shifts in customer behavior, requiring deeper investigation.

**Customer Behavior & Retention Dynamics**
- Customer retention is very low, with **most customers churning after the first month**, except for an unusual spike in Cohorts 1 and 2 - possibly linked to a campaign.
- However, given the long lifecycle of bicycles, a low retention rate is expected and reflects the nature of the industry.

**Inventory Imbalances & Operational Signals**
- Inventory levels show significant volatility, with both **stockouts** (risking lost sales) and **overstocking** (raising storage costs and markdown risks).
- Inventory management appears misaligned with actual demand, as seen in products like **HL Mountain Frame (Black & Silver)**, which exhibit very high stock-to-sales ratios and require close monitoring.
- On a positive note, no pending orders were recorded in 2014, highlighting **strong operational efficiency** in order processing.

## 🚀 Recommendations
-	**Diversify product portfolio and market coverage**: Expand into new subcategories and strengthen underperforming territories to reduce overdependence on a few revenue drivers.
-	**Conduct deeper market research**: Investigate the root causes of performance decline across all subcategories (e.g., demand shifts, competition, customer behavior changes, global recession) to inform product and marketing strategies.
-	**Pause discount-heavy strategies**: Avoid broad discount campaigns in the current low-growth environment, as they may further pressure profitability. Instead, adopt targeted promotions tied to clear customer acquisition or cross-selling goals.
-	**Refocus growth strategy on new customer acquisition and upselling**: Given the long lifecycle of bicycles and naturally low purchase frequency, prioritize attracting new customers through stronger brand awareness and expanded distribution. Drive higher order value with upsell tactics, bundles, and service add-ons (e.g., maintenance plans, accessory packages).
