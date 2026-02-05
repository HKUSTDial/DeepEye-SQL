WITH unicorns AS (
  SELECT 
    cd."company_id",
    EXTRACT(YEAR FROM TO_DATE(cd."date_joined")) AS join_year,
    ci."industry",
    cf."valuation"
  FROM "MODERN_DATA"."MODERN_DATA"."COMPANIES_DATES" cd
  INNER JOIN "MODERN_DATA"."MODERN_DATA"."COMPANIES_FUNDING" cf 
    ON cd."company_id" = cf."company_id"
  INNER JOIN "MODERN_DATA"."MODERN_DATA"."COMPANIES_INDUSTRIES" ci 
    ON cd."company_id" = ci."company_id"
  WHERE cf."valuation" >= 1000000000
    AND EXTRACT(YEAR FROM TO_DATE(cd."date_joined")) BETWEEN 2019 AND 2021
),
industry_year_counts AS (
  SELECT 
    "industry",
    join_year,
    COUNT(DISTINCT "company_id") AS num_companies
  FROM unicorns
  GROUP BY "industry", join_year
),
industry_totals AS (
  SELECT 
    "industry",
    SUM(num_companies) AS total_companies
  FROM industry_year_counts
  GROUP BY "industry"
),
top_industry AS (
  SELECT "industry"
  FROM industry_totals
  ORDER BY total_companies DESC
  LIMIT 1
),
years AS (
  SELECT 2019 AS year UNION ALL SELECT 2020 UNION ALL SELECT 2021
),
top_industry_years AS (
  SELECT ti."industry", y.year
  FROM top_industry ti
  CROSS JOIN years y
),
top_industry_counts AS (
  SELECT 
    tiy."industry",
    tiy.year,
    COALESCE(iyc.num_companies, 0) AS num_companies
  FROM top_industry_years tiy
  LEFT JOIN industry_year_counts iyc 
    ON tiy."industry" = iyc."industry" AND tiy.year = iyc.join_year
)
SELECT 
  "industry",
  AVG(num_companies) AS avg_new_unicorns_per_year
FROM top_industry_counts
GROUP BY "industry"