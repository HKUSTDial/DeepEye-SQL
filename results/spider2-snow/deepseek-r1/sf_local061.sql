WITH historical_sales AS (
  SELECT 
    s."prod_id",
    t."calendar_year",
    t."calendar_month_number",
    SUM(s."amount_sold") AS total_sales
  FROM "COMPLEX_ORACLE"."COMPLEX_ORACLE"."SALES" s
  JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CUSTOMERS" c ON s."cust_id" = c."cust_id"
  JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COUNTRIES" co ON c."country_id" = co."country_id"
  JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."TIMES" t ON s."time_id" = t."time_id"
  JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."PROMOTIONS" p ON s."promo_id" = p."promo_id"
  JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CHANNELS" ch ON s."channel_id" = ch."channel_id"
  WHERE co."country_name" = 'France'
    AND p."promo_total_id" = 1
    AND ch."channel_total_id" = 1
    AND t."calendar_year" IN (2019, 2020)
  GROUP BY s."prod_id", t."calendar_year", t."calendar_month_number"
), pivot_sales AS (
  SELECT 
    "prod_id",
    "calendar_month_number",
    SUM(CASE WHEN "calendar_year" = 2019 THEN total_sales ELSE 0 END) AS sales_2019,
    SUM(CASE WHEN "calendar_year" = 2020 THEN total_sales ELSE 0 END) AS sales_2020
  FROM historical_sales
  GROUP BY "prod_id", "calendar_month_number"
  HAVING sales_2019 > 0 AND sales_2020 > 0
), projected_sales AS (
  SELECT 
    "prod_id",
    "calendar_month_number",
    sales_2019,
    sales_2020,
    (sales_2020 * (sales_2020 / NULLIF(sales_2019, 0))) AS projected_sales_2021
  FROM pivot_sales
), with_exchange AS (
  SELECT 
    p."prod_id",
    p."calendar_month_number",
    p.projected_sales_2021,
    COALESCE(c."to_us", 1) AS exchange_rate,
    p.projected_sales_2021 * COALESCE(c."to_us", 1) AS projected_usd
  FROM projected_sales p
  LEFT JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CURRENCY" c 
    ON c."month" = p."calendar_month_number" 
    AND c."year" = 2021 
    AND c."country" = 'France'
)
SELECT 
  "calendar_month_number" AS month,
  AVG(projected_usd) AS avg_projected_monthly_sales_usd
FROM with_exchange
GROUP BY "calendar_month_number"
ORDER BY month