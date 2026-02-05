WITH state_year_totals AS (
  SELECT
    SPLIT_PART("CoC_Number", '-', 1) AS "state_abbr",
    "Count_Year",
    SUM("Unsheltered_Homeless") AS "total_unsheltered"
  FROM "SDOH"."SDOH_HUD_PIT_HOMELESSNESS"."HUD_PIT_BY_COC"
  WHERE "Count_Year" IN (2015, 2018)
  GROUP BY "state_abbr", "Count_Year"
),
state_totals_pivot AS (
  SELECT
    "state_abbr",
    MAX(CASE WHEN "Count_Year" = 2015 THEN "total_unsheltered" END) AS "total_2015",
    MAX(CASE WHEN "Count_Year" = 2018 THEN "total_unsheltered" END) AS "total_2018"
  FROM state_year_totals
  GROUP BY "state_abbr"
  HAVING "total_2015" IS NOT NULL AND "total_2018" IS NOT NULL AND "total_2015" > 0
),
state_pct_change AS (
  SELECT
    "state_abbr",
    ("total_2018" - "total_2015") * 100.0 / "total_2015" AS "pct_change"
  FROM state_totals_pivot
),
national_avg AS (
  SELECT AVG("pct_change") AS "avg_pct_change"
  FROM state_pct_change
),
state_diff AS (
  SELECT
    s."state_abbr",
    ABS(s."pct_change" - n."avg_pct_change") AS "diff"
  FROM state_pct_change s
  CROSS JOIN national_avg n
)
SELECT "state_abbr"
FROM state_diff
ORDER BY "diff" ASC
LIMIT 5