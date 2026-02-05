WITH "all_combos" AS (
  SELECT 2021 AS "year" UNION ALL SELECT 2022 UNION ALL SELECT 2023
),
"months" AS (
  SELECT 4 AS "month" UNION ALL SELECT 5 UNION ALL SELECT 6
),
"all_year_month" AS (
  SELECT "year", "month" FROM "all_combos" CROSS JOIN "months"
),
"actual_counts" AS (
  SELECT 
    EXTRACT(YEAR FROM TO_DATE("insert_date", 'YYYY-MM-DD')) AS "year",
    EXTRACT(MONTH FROM TO_DATE("insert_date", 'YYYY-MM-DD')) AS "month",
    COUNT(*) AS "monthly_count"
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."CITIES"
  WHERE EXTRACT(YEAR FROM TO_DATE("insert_date", 'YYYY-MM-DD')) BETWEEN 2021 AND 2023
    AND EXTRACT(MONTH FROM TO_DATE("insert_date", 'YYYY-MM-DD')) IN (4,5,6)
  GROUP BY "year", "month"
),
"monthly_counts" AS (
  SELECT 
    a."year",
    a."month",
    COALESCE(c."monthly_count", 0) AS "monthly_count"
  FROM "all_year_month" a
  LEFT JOIN "actual_counts" c ON a."year" = c."year" AND a."month" = c."month"
),
"running_totals" AS (
  SELECT 
    "year",
    "month",
    "monthly_count",
    SUM("monthly_count") OVER (PARTITION BY "month" ORDER BY "year" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "running_total"
  FROM "monthly_counts"
),
"with_prev" AS (
  SELECT 
    "year",
    "month",
    "monthly_count",
    "running_total",
    LAG("monthly_count") OVER (PARTITION BY "month" ORDER BY "year") AS "prev_monthly",
    LAG("running_total") OVER (PARTITION BY "month" ORDER BY "year") AS "prev_running"
  FROM "running_totals"
)
SELECT 
  "year",
  CASE "month" 
    WHEN 4 THEN 'April'
    WHEN 5 THEN 'May'
    WHEN 6 THEN 'June'
  END AS "month",
  "monthly_count" AS "total_cities_added",
  "running_total" AS "cumulative_running_total",
  ROUND(100.0 * ("monthly_count" - "prev_monthly") / NULLIF("prev_monthly", 0), 2) AS "monthly_growth_pct",
  ROUND(100.0 * ("running_total" - "prev_running") / NULLIF("prev_running", 0), 2) AS "running_growth_pct"
FROM "with_prev"
WHERE "year" IN (2022, 2023)
ORDER BY "year", "month"