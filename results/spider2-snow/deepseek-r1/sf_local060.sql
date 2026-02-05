WITH us_country AS (
  SELECT "country_id" FROM "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COUNTRIES" WHERE "country_name" LIKE '%United States%'
),
us_sales_q4 AS (
  SELECT s."cust_id", s."prod_id", s."amount_sold", c."cust_city_id", c."cust_city", t."calendar_year"
  FROM "COMPLEX_ORACLE"."COMPLEX_ORACLE"."SALES" s
  JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CUSTOMERS" c ON s."cust_id" = c."cust_id"
  JOIN us_country co ON c."country_id" = co."country_id"
  JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."TIMES" t ON s."time_id" = t."time_id"
  WHERE t."calendar_year" IN (2019, 2020) AND t."calendar_quarter_number" = 4 AND s."promo_id" = 999
),
city_quarterly_sales AS (
  SELECT "cust_city_id", "cust_city", "calendar_year", SUM("amount_sold") AS total_sales
  FROM us_sales_q4
  GROUP BY "cust_city_id", "cust_city", "calendar_year"
),
city_sales_pivot AS (
  SELECT "cust_city_id", "cust_city", SUM(CASE WHEN "calendar_year" = 2019 THEN total_sales ELSE 0 END) AS sales_2019, SUM(CASE WHEN "calendar_year" = 2020 THEN total_sales ELSE 0 END) AS sales_2020
  FROM city_quarterly_sales
  GROUP BY "cust_city_id", "cust_city"
),
cities_meeting_growth AS (
  SELECT "cust_city_id", "cust_city", sales_2019, sales_2020
  FROM city_sales_pivot
  WHERE sales_2020 >= sales_2019 * 1.2 AND sales_2019 > 0
),
sales_in_growth_cities AS (
  SELECT us."prod_id", us."calendar_year", us."amount_sold"
  FROM us_sales_q4 us
  JOIN cities_meeting_growth ct ON us."cust_city_id" = ct."cust_city_id"
),
product_total_sales AS (
  SELECT "prod_id", SUM("amount_sold") AS overall_sales
  FROM sales_in_growth_cities
  GROUP BY "prod_id"
),
product_rank AS (
  SELECT "prod_id", overall_sales, ROW_NUMBER() OVER (ORDER BY overall_sales DESC) AS rn, COUNT(*) OVER () AS total_count
  FROM product_total_sales
),
top_products AS (
  SELECT "prod_id" FROM product_rank WHERE rn <= CEIL(total_count * 0.2)
),
quarter_totals AS (
  SELECT "calendar_year", SUM("amount_sold") AS quarter_total
  FROM sales_in_growth_cities
  GROUP BY "calendar_year"
),
product_quarter_sales AS (
  SELECT "prod_id", "calendar_year", SUM("amount_sold") AS product_sales
  FROM sales_in_growth_cities
  GROUP BY "prod_id", "calendar_year"
),
top_products_years AS (
  SELECT tp."prod_id", y."calendar_year"
  FROM top_products tp
  CROSS JOIN (SELECT DISTINCT "calendar_year" FROM sales_in_growth_cities) y
),
top_product_quarters_data AS (
  SELECT tpy."prod_id", tpy."calendar_year", COALESCE(pqs.product_sales, 0) AS product_sales, qt.quarter_total
  FROM top_products_years tpy
  LEFT JOIN product_quarter_sales pqs ON tpy."prod_id" = pqs."prod_id" AND tpy."calendar_year" = pqs."calendar_year"
  JOIN quarter_totals qt ON tpy."calendar_year" = qt."calendar_year"
),
product_shares AS (
  SELECT "prod_id", SUM(CASE WHEN "calendar_year" = 2019 THEN product_sales / quarter_total ELSE 0 END) AS share_2019, SUM(CASE WHEN "calendar_year" = 2020 THEN product_sales / quarter_total ELSE 0 END) AS share_2020
  FROM top_product_quarters_data
  GROUP BY "prod_id"
),
product_share_change AS (
  SELECT "prod_id", share_2019, share_2020, share_2020 - share_2019 AS share_change
  FROM product_shares
)
SELECT p."prod_name", ps.share_2019, ps.share_2020, ps.share_change
FROM product_share_change ps
JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."PRODUCTS" p ON ps."prod_id" = p."prod_id"
ORDER BY ps.share_change DESC;