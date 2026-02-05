WITH years AS (
  SELECT 2016 AS "year" UNION ALL
  SELECT 2017 UNION ALL
  SELECT 2018
), delivered_orders AS (
  SELECT 
    "order_id",
    delivered_date
  FROM (
    SELECT 
      "order_id",
      TRY_TO_DATE("order_delivered_customer_date") AS delivered_date
    FROM 
      "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDERS"
    WHERE 
      "order_status" = 'delivered'
      AND "order_delivered_customer_date" IS NOT NULL
  )
  WHERE delivered_date IS NOT NULL
), annual_counts AS (
  SELECT 
    y."year",
    COUNT(o."order_id") AS annual_volume
  FROM 
    years y
  LEFT JOIN 
    delivered_orders o ON YEAR(o.delivered_date) = y."year"
  GROUP BY 
    y."year"
), min_year AS (
  SELECT "year"
  FROM annual_counts
  ORDER BY annual_volume ASC, "year" ASC
  LIMIT 1
), monthly_counts AS (
  SELECT 
    YEAR(o.delivered_date) AS "year",
    MONTH(o.delivered_date) AS "month",
    COUNT(*) AS monthly_volume
  FROM 
    delivered_orders o
  WHERE 
    YEAR(o.delivered_date) = (SELECT "year" FROM min_year)
  GROUP BY 
    YEAR(o.delivered_date), MONTH(o.delivered_date)
)
SELECT 
  COALESCE(MAX(monthly_volume), 0) AS highest_monthly_volume
FROM 
  monthly_counts