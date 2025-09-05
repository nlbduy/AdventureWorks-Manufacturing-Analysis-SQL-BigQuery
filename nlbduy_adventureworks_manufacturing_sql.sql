-- Query 1
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

-- Query 2
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

-- Query 3
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

-- Query 4
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

-- Query 5
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

-- Query 6
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

-- Query 7
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

-- Query 8
SELECT
  EXTRACT(YEAR FROM ModifiedDate) AS year
  , Status
  , COUNT(DISTINCT SalesOrderID) AS order_count
  , SUM(TotalDue) AS total_value
FROM `adventureworks2019.Sales.SalesOrderHeader`
WHERE EXTRACT(YEAR FROM ModifiedDate) = 2014 AND Status = 1
GROUP BY year, Status