WITH historical_sales AS (
  SELECT 
    s."prod_id",
    t."calendar_year",
    t."calendar_month_number",
    SUM(s."amount_sold") AS total_sales
  FROM 
    "COMPLEX_ORACLE"."COMPLEX_ORACLE"."SALES" s
  INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."TIMES" t 
    ON s."time_id" = t."time_id"
  INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CUSTOMERS" c 
    ON s."cust_id" = c."cust_id"
  INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COUNTRIES" co 
    ON c."country_id" = co."country_id"
  INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."PROMOTIONS" p 
    ON s."promo_id" = p."promo_id"
  INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CHANNELS" ch 
    ON s."channel_id" = ch."channel_id"
  WHERE 
    co."country_name" = 'France'
    AND p."promo_total_id" = 1
    AND ch."channel_total_id" = 1
    AND t."calendar_year" IN (2019, 2020)
  GROUP BY 
    s."prod_id", t."calendar_year", t."calendar_month_number"
),
sales_pivot AS (
  SELECT 
    "prod_id",
    "calendar_month_number",
    MAX(CASE WHEN "calendar_year" = 2019 THEN total_sales END) AS sales_2019,
    MAX(CASE WHEN "calendar_year" = 2020 THEN total_sales END) AS sales_2020
  FROM historical_sales
  GROUP BY "prod_id", "calendar_month_number"
),
projected_2021 AS (
  SELECT 
    "prod_id",
    "calendar_month_number",
    CASE 
      WHEN sales_2019 > 0 THEN (sales_2020 * sales_2020 / sales_2019)
      ELSE NULL 
    END AS projected_sales
  FROM sales_pivot
  WHERE sales_2019 IS NOT NULL AND sales_2020 IS NOT NULL
),
converted AS (
  SELECT 
    p."prod_id",
    p."calendar_month_number",
    p.projected_sales * COALESCE(c."to_us", 1) AS projected_usd
  FROM projected_2021 p
  LEFT JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CURRENCY" c 
    ON p."calendar_month_number" = c."month"
    AND c."year" = 2021
    AND c."country" = 'France'
),
monthly_averages AS (
  SELECT 
    "calendar_month_number",
    AVG(projected_usd) AS avg_monthly_projected_usd
  FROM converted
  GROUP BY "calendar_month_number"
)
SELECT MEDIAN(avg_monthly_projected_usd) AS median_avg_monthly_projected_usd
FROM monthly_averages