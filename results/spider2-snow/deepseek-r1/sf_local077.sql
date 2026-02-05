WITH monthly_avg_composition AS (
  SELECT 
    im."month_year",
    im."_year",
    im."_month",
    im."interest_id",
    m."interest_name",
    im."composition" / im."index_value" AS "avg_composition"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."INTEREST_METRICS" im
  INNER JOIN "BANK_SALES_TRADING"."BANK_SALES_TRADING"."INTEREST_MAP" m
    ON CAST(im."interest_id" AS NUMBER) = m."id"
  WHERE (im."_year" * 100 + im."_month") BETWEEN 201809 AND 201908
    AND im."composition" IS NOT NULL
    AND im."index_value" IS NOT NULL
    AND im."index_value" != 0
),
ranked_interests AS (
  SELECT 
    "month_year",
    "_year",
    "_month",
    "interest_id",
    "interest_name",
    "avg_composition",
    ROW_NUMBER() OVER (PARTITION BY "month_year" ORDER BY "avg_composition" DESC) AS "rank"
  FROM monthly_avg_composition
),
monthly_max_composition AS (
  SELECT 
    "month_year",
    "_year",
    "_month",
    "interest_id",
    "interest_name",
    "avg_composition" AS "max_index_composition"
  FROM ranked_interests
  WHERE "rank" = 1
),
with_lagged_values AS (
  SELECT 
    "month_year",
    "_year",
    "_month",
    "interest_id",
    "interest_name",
    "max_index_composition",
    LAG("interest_name", 1) OVER (ORDER BY "_year", "_month") AS "prev_1_interest_name",
    LAG("max_index_composition", 1) OVER (ORDER BY "_year", "_month") AS "prev_1_max_index_composition",
    LAG("interest_name", 2) OVER (ORDER BY "_year", "_month") AS "prev_2_interest_name",
    LAG("max_index_composition", 2) OVER (ORDER BY "_year", "_month") AS "prev_2_max_index_composition",
    AVG("max_index_composition") OVER (ORDER BY "_year", "_month" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "rolling_avg"
  FROM monthly_max_composition
)
SELECT 
  DATE_FROM_PARTS("_year", "_month", 1) AS "date",
  "interest_name",
  "max_index_composition",
  "rolling_avg",
  "prev_1_interest_name",
  "prev_1_max_index_composition",
  "prev_2_interest_name",
  "prev_2_max_index_composition"
FROM with_lagged_values
ORDER BY "_year", "_month"