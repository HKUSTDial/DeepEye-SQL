WITH target_weeks AS (
  SELECT 2018 AS "year", EXTRACT(WEEK FROM DATE_FROM_PARTS(2018, 6, 15)) AS "target_week"
  UNION ALL
  SELECT 2019, EXTRACT(WEEK FROM DATE_FROM_PARTS(2019, 6, 15))
  UNION ALL
  SELECT 2020, EXTRACT(WEEK FROM DATE_FROM_PARTS(2020, 6, 15))
)
SELECT 
  "year",
  "sales_before",
  "sales_after",
  ( "sales_after" - "sales_before" ) / NULLIF("sales_before", 0) * 100 AS "pct_change"
FROM (
  SELECT 
    tw."year",
    SUM(CASE WHEN cws."week_number" BETWEEN tw."target_week" - 4 AND tw."target_week" - 1 THEN cws."sales" ELSE 0 END) AS "sales_before",
    SUM(CASE WHEN cws."week_number" BETWEEN tw."target_week" + 1 AND tw."target_week" + 4 THEN cws."sales" ELSE 0 END) AS "sales_after"
  FROM target_weeks tw
  LEFT JOIN "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CLEANED_WEEKLY_SALES" cws
    ON cws."calendar_year" = tw."year"
  WHERE tw."year" IN (2018, 2019, 2020)
  GROUP BY tw."year", tw."target_week"
) subquery
ORDER BY "year"