WITH wage_data AS (
  SELECT 1998 AS "year", "avg_wkly_wage_10_total_all_industries" AS "avg_wkly_wage"
  FROM "BLS"."BLS_QCEW"."_1998_Q1"
  WHERE "area_fips" = '42003'
  UNION ALL
  SELECT 1998, "avg_wkly_wage_10_total_all_industries"
  FROM "BLS"."BLS_QCEW"."_1998_Q2"
  WHERE "area_fips" = '42003'
  UNION ALL
  SELECT 1998, "avg_wkly_wage_10_total_all_industries"
  FROM "BLS"."BLS_QCEW"."_1998_Q3"
  WHERE "area_fips" = '42003'
  UNION ALL
  SELECT 1998, "avg_wkly_wage_10_total_all_industries"
  FROM "BLS"."BLS_QCEW"."_1998_Q4"
  WHERE "area_fips" = '42003'
  UNION ALL
  SELECT 2017, "avg_wkly_wage_10_total_all_industries"
  FROM "BLS"."BLS_QCEW"."_2017_Q1"
  WHERE "area_fips" = '42003'
  UNION ALL
  SELECT 2017, "avg_wkly_wage_10_total_all_industries"
  FROM "BLS"."BLS_QCEW"."_2017_Q2"
  WHERE "area_fips" = '42003'
  UNION ALL
  SELECT 2017, "avg_wkly_wage_10_total_all_industries"
  FROM "BLS"."BLS_QCEW"."_2017_Q3"
  WHERE "area_fips" = '42003'
  UNION ALL
  SELECT 2017, "avg_wkly_wage_10_total_all_industries"
  FROM "BLS"."BLS_QCEW"."_2017_Q4"
  WHERE "area_fips" = '42003'
),
yearly_wage AS (
  SELECT "year", AVG("avg_wkly_wage") AS "avg_weekly_wage"
  FROM wage_data
  GROUP BY "year"
),
wage_growth AS (
  SELECT 
    (MAX(CASE WHEN "year" = 2017 THEN "avg_weekly_wage" END) - MIN(CASE WHEN "year" = 1998 THEN "avg_weekly_wage" END)) / MIN(CASE WHEN "year" = 1998 THEN "avg_weekly_wage" END) * 100 AS "wage_pct_growth"
  FROM yearly_wage
),
cpi_data AS (
  SELECT "year", "value" AS "cpi_index"
  FROM "BLS"."BLS"."CPI_U"
  WHERE "area_name" LIKE '%Pittsburgh%'
    AND "item_name" LIKE '%All items%'
    AND "period" = 'M13'
    AND "year" IN (1998, 2017)
),
cpi_growth AS (
  SELECT 
    (MAX(CASE WHEN "year" = 2017 THEN "cpi_index" END) - MIN(CASE WHEN "year" = 1998 THEN "cpi_index" END)) / MIN(CASE WHEN "year" = 1998 THEN "cpi_index" END) * 100 AS "cpi_pct_growth"
  FROM cpi_data
)
SELECT 
  ROUND("wage_pct_growth", 2) AS "wage_growth_pct",
  ROUND("cpi_pct_growth", 2) AS "cpi_growth_pct",
  CASE WHEN "wage_pct_growth" >= "cpi_pct_growth" THEN 'Yes' ELSE 'No' END AS "kept_pace"
FROM wage_growth, cpi_growth