WITH long_data AS (
  SELECT 'region' AS attribute_type, "region" AS attribute_value, "sales", "week_date"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CLEANED_WEEKLY_SALES"
  UNION ALL
  SELECT 'platform', "platform", "sales", "week_date"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CLEANED_WEEKLY_SALES"
  UNION ALL
  SELECT 'age_band', "age_band", "sales", "week_date"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CLEANED_WEEKLY_SALES"
  UNION ALL
  SELECT 'demographic', "demographic", "sales", "week_date"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CLEANED_WEEKLY_SALES"
  UNION ALL
  SELECT 'customer_type', "customer_type", "sales", "week_date"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CLEANED_WEEKLY_SALES"
), filtered_dates AS (
  SELECT attribute_type, attribute_value, "sales",
         TO_DATE("week_date", 'YYYY-MM-DD') AS week_date_dt
  FROM long_data
  WHERE week_date_dt BETWEEN '2020-03-23' AND '2020-09-06'
), period_sales AS (
  SELECT attribute_type, attribute_value,
         SUM(CASE WHEN week_date_dt < '2020-06-15' THEN "sales" ELSE 0 END) AS before_sales,
         SUM(CASE WHEN week_date_dt >= '2020-06-15' THEN "sales" ELSE 0 END) AS after_sales
  FROM filtered_dates
  GROUP BY attribute_type, attribute_value
), percentage_changes AS (
  SELECT attribute_type, attribute_value,
         (after_sales - before_sales) * 100.0 / NULLIF(before_sales, 0) AS pct_change
  FROM period_sales
  WHERE before_sales > 0
), avg_by_type AS (
  SELECT attribute_type,
         AVG(pct_change) AS avg_pct_change
  FROM percentage_changes
  GROUP BY attribute_type
)
SELECT attribute_type, avg_pct_change
FROM avg_by_type
ORDER BY avg_pct_change ASC
LIMIT 1