-- =====================================
-- Project: Retail Business Performance & Profitability Analysis
-- File   : retail_analysis_queries.sql
-- Author : Vishal Kumar
-- Date   : 2025-08-23
-- INTERNSHIP PROJECT PHASE (2 WEEKS)
-- =====================================

select * from superstore;

-- 👉 DDL = Data Definition Language
CREATE TABLE `superstore_analyse` (
  `Row ID` int DEFAULT NULL,
  `Order ID` text,
  `Order Date` text,
  `Ship Date` text,
  `Ship Mode` text,
  `Customer ID` text,
  `Customer Name` text,
  `Segment` text,
  `Country` text,
  `City` text,
  `State` text,
  `Postal Code` int DEFAULT NULL,
  `Region` text,
  `Product ID` text,
  `Category` text,
  `Sub-Category` text,
  `Product Name` text,
  `Sales` double DEFAULT NULL,
  `Quantity` int DEFAULT NULL,
  `Discount` double DEFAULT NULL,
  `Profit` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from superstore_analyse;

-- 2) Load data
INSERT superstore_analyse
SELECT * from superstore;

-- 3) Basic cleaning 
-- Remove rows where sales or profit is NULL
DELETE FROM superstore_analyse 
WHERE sales IS NULL OR profit IS NULL;

--  Drop rows with missing order_date
DELETE FROM superstore_analyse WHERE order_date IS NULL;


-- 4) Profit margins by category & sub-category
SELECT
  'category', 'sub-category',
  round(SUM(sales),2) AS sales,
  round(SUM(profit),2) AS profit,
	  CASE WHEN SUM(sales) > 0 
	  THEN round(SUM(profit)/SUM(sales),2)
	  END AS margin_pct,
  round(AVG(discount),2) AS avg_discount,
  COUNT(DISTINCT 'order id') AS orders
FROM superstore_analyse
GROUP BY 'category', 'sub-category'
ORDER BY profit ASC, margin_pct ASC
LIMIT 25;

select * from superstore_analyse;


-- 5) Enriched view with helpful derived fields
CREATE OR REPLACE VIEW superstore_analyzing2 AS
SELECT
  *,
  CASE WHEN sales > 0 THEN profit / sales ELSE NULL END AS profit_margin,
  EXTRACT(YEAR FROM `order date`) AS order_year,
  EXTRACT(MONTH FROM `order date`) AS order_month,
  CASE
    WHEN EXTRACT(MONTH FROM `order date`) IN (12,1,2) THEN 'Winter'
    WHEN EXTRACT(MONTH FROM `order date`) IN (3,4,5) THEN 'Spring'
    WHEN EXTRACT(MONTH FROM `order date`) IN (6,7,8) THEN 'Summer'
    WHEN EXTRACT(MONTH FROM `order date`) IN (9,10,11) THEN 'Autumn'
  END AS season,
  CASE
    WHEN `ship date` IS NOT NULL AND `order date` IS NOT NULL
    THEN DATEDIFF(`ship date`, `order date`)
    ELSE NULL
  END AS days_to_ship
FROM superstore_analyse;



-- 6) Seasonality: share of yearly sales by month within each sub-category
WITH monthly AS (
  SELECT 
    `Sub-Category` AS sub_category,
    MONTH(STR_TO_DATE(`Order Date`, '%m/%d/%Y')) AS order_month,   -- convert text → date
    round(SUM(Sales),2) AS monthly_sales
  FROM superstore_analyzing2
  GROUP BY `Sub-Category`, MONTH(STR_TO_DATE(`Order Date`, '%m/%d/%Y'))
),
totals AS (
  SELECT 
    sub_category,
    round(SUM(monthly_sales),2) AS total_sales
  FROM monthly
  GROUP BY sub_category
)
SELECT
  m.sub_category,
  m.order_month,
  m.monthly_sales,
  t.total_sales,
  ROUND(m.monthly_sales / t.total_sales, 4) AS month_share_of_year
FROM monthly m
JOIN totals t 
  ON m.sub_category = t.sub_category
ORDER BY m.sub_category, m.order_month;


-- ✅ Step 7 (Velocity proxy – avg days between orders)
WITH ordered AS (
  SELECT
    `Product Name` AS product_name,
    STR_TO_DATE(`Order Date`, '%m/%d/%Y') AS order_date,
    ROW_NUMBER() OVER (PARTITION BY `Product Name` ORDER BY STR_TO_DATE(`Order Date`, '%m/%d/%Y')) AS rn
  FROM superstore_analyse
  WHERE `Order Date` IS NOT NULL
),
pairs AS (
  SELECT
    o1.product_name,
    o1.order_date AS this_order,
    o2.order_date AS next_order,
    DATEDIFF(o2.order_date, o1.order_date) AS days_between
  FROM ordered o1
  JOIN ordered o2
    ON o1.product_name = o2.product_name
   AND o2.rn = o1.rn + 1
)
SELECT
  product_name,
  ROUND(AVG(days_between),2) AS avg_days_between_orders
FROM pairs
GROUP BY product_name
ORDER BY avg_days_between_orders DESC
LIMIT 100;


-- ✅ Step 8 (Slow-moving / overstocked heuristic flags)
WITH base AS (
  SELECT
    `Sub-Category` AS sub_category,
    COUNT(DISTINCT `Order ID`) AS orders,
    AVG(Discount) AS avg_discount,
    SUM(Sales) AS sales,
    SUM(Profit) AS profit,
    CASE WHEN SUM(Sales) > 0 THEN SUM(Profit)/SUM(Sales) END AS margin_pct
  FROM superstore_analyse
  GROUP BY `Sub-Category`
),
ranked AS (
  SELECT 
    b.*,
    PERCENT_RANK() OVER (ORDER BY orders) AS orders_percentile,
    PERCENT_RANK() OVER (ORDER BY avg_discount) AS discount_percentile
  FROM base b
)
SELECT
  sub_category,
  orders,
  avg_discount,
  sales,
  profit,
  ROUND(margin_pct,4) AS margin_pct,
  CASE WHEN orders_percentile <= 0.25 AND discount_percentile >= 0.75 THEN 1 ELSE 0 END AS slow_moving_flag,
  CASE WHEN avg_discount >= 0.30 AND COALESCE(margin_pct,0) <= 0.05 THEN 1 ELSE 0 END AS over_discounted_flag
FROM ranked
ORDER BY slow_moving_flag DESC, over_discounted_flag DESC, margin_pct ASC, profit ASC;



-- 9.Region Trends
SELECT Region,
       round(SUM(Sales),2) AS TotalSales,
       round(SUM(Profit),2) AS TotalProfit,
       ROUND((SUM(Profit)/SUM(Sales))*100,2) AS ProfitMarginPercent
FROM SuperStore
GROUP BY Region;

--------------------------------------------------------------------------------------------------------------------------------------
## Re-check

DROP TABLE IF EXISTS `superstore_enriched`;

CREATE TABLE `superstore_enriched` AS
SELECT
  `Row ID`                AS row_id,
  `Order ID`              AS order_id,
  STR_TO_DATE(`Order Date`,'%m/%d/%Y') AS order_date,
  STR_TO_DATE(`Ship Date` ,'%m/%d/%Y') AS ship_date,
  `Ship Mode`             AS ship_mode,
  `Customer ID`           AS customer_id,
  `Customer Name`         AS customer_name,
  `Segment`               AS segment,
  `Country`               AS country,
  `City`                  AS city,
  `State`                 AS state,
  `Postal Code`           AS postal_code,
  `Region`                AS region,
  `Product ID`            AS product_id,
  `Category`              AS category,
  `Sub-Category`          AS sub_category,
  `Product Name`          AS product_name,
  Sales                   AS sales,
  Quantity                AS quantity,
  Discount                AS discount,
  Profit                  AS profit,
  CASE WHEN Sales > 0 THEN Profit / Sales ELSE NULL END AS profit_margin,
  YEAR(STR_TO_DATE(`Order Date`,'%m/%d/%Y'))  AS order_year,
  MONTH(STR_TO_DATE(`Order Date`,'%m/%d/%Y')) AS order_month,
  CASE
    WHEN MONTH(STR_TO_DATE(`Order Date`,'%m/%d/%Y')) IN (12,1,2) THEN 'Winter'
    WHEN MONTH(STR_TO_DATE(`Order Date`,'%m/%d/%Y')) IN (3,4,5) THEN 'Spring'
    WHEN MONTH(STR_TO_DATE(`Order Date`,'%m/%d/%Y')) IN (6,7,8) THEN 'Summer'
    ELSE 'Autumn'
  END AS season,
  DATEDIFF(STR_TO_DATE(`Ship Date` ,'%m/%d/%Y'), STR_TO_DATE(`Order Date`,'%m/%d/%Y')) AS days_to_ship
FROM superstore_analyse
WHERE Sales IS NOT NULL AND Profit IS NOT NULL;

-- Add indexes to speed up queries:

CREATE INDEX idx_order_date ON superstore_enriched(order_date);
CREATE INDEX idx_subcat ON superstore_enriched(sub_category);
CREATE INDEX idx_product ON superstore_enriched(product_id);
CREATE INDEX idx_customer ON superstore_enriched(customer_id);
CREATE INDEX idx_region ON superstore_enriched(region);


-- 2) Top / Bottom products (revenue & profit)
-- Top N and bottom N by profi
SELECT product_id, product_name,
       ROUND(SUM(sales),2) AS total_sales,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(AVG(profit_margin),4) AS avg_margin,
       SUM(quantity) AS total_qty,
       ROUND(AVG(discount),4) AS avg_discount
FROM superstore_enriched
GROUP BY product_id, product_name
ORDER BY total_profit DESC
LIMIT 50;


-- Bottom products (largest negative profit or lowest margin):

SELECT product_id, product_name,
       ROUND(SUM(sales),2) AS total_sales,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(AVG(profit_margin),4) AS avg_margin,
       SUM(quantity) AS total_qty,
       ROUND(AVG(discount),4) AS avg_discount
FROM superstore_enriched
GROUP BY product_id, product_name
HAVING SUM(sales) > 0
ORDER BY total_profit ASC
LIMIT 50;


-- 3) Customer segments & top customers (CLV)

SELECT segment,
       COUNT(DISTINCT customer_id) AS unique_customers,
       ROUND(SUM(sales),2) AS total_sales,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(SUM(profit)/SUM(sales)*100,2) AS margin_pct
FROM superstore_enriched
GROUP BY segment
ORDER BY total_sales DESC;

-- Top customers (CLV proxy):
SELECT customer_id, customer_name,
       COUNT(DISTINCT order_id) AS orders,
       ROUND(SUM(sales),2) AS total_sales,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(AVG(profit_margin),4) AS avg_margin
FROM superstore_enriched
GROUP BY customer_id, customer_name
ORDER BY total_sales DESC
LIMIT 50;



-- 4) Category / Sub-category insights (profitability + discounts)
SELECT category, sub_category,
       ROUND(SUM(sales),2) AS total_sales,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(SUM(profit)/SUM(sales)*100,2) AS margin_pct,
       ROUND(AVG(discount),4) AS avg_discount,
       COUNT(DISTINCT product_id) AS sku_count
FROM superstore_enriched
GROUP BY category, sub_category
ORDER BY total_profit ASC, margin_pct ASC;


-- 5) Region-wise trends (your query, plus monthly time series)
SELECT Region,
       ROUND(SUM(Sales),2) AS TotalSales,
       ROUND(SUM(Profit),2) AS TotalProfit,
       ROUND((SUM(Profit)/SUM(Sales))*100,2) AS ProfitMarginPercent
FROM superstore_enriched
GROUP BY Region;


-- Region monthly trend:

SELECT region, order_year, order_month,
       ROUND(SUM(sales),2) AS total_sales,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(SUM(profit)/SUM(sales)*100,2) AS margin_pct
FROM superstore_enriched
GROUP BY region, order_year, order_month
ORDER BY region, order_year, order_month;


