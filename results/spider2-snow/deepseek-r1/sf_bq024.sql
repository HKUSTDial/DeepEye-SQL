WITH condition_acres AS (
  SELECT
    "A"."evaluation_group",
    "A"."evaluation_type",
    "C"."condition_status_code",
    "A"."evaluation_description",
    "A"."state_code",
    "A"."macroplot_acres",
    "A"."subplot_acres" AS "total_subplot_acres",
    "C"."condition_proportion_unadjusted",
    "A"."subplot_acres" * "C"."condition_proportion_unadjusted" AS "condition_subplot_acres"
  FROM "USFS_FIA"."USFS_FIA"."ESTIMATED_FORESTLAND_ACRES" AS "A"
  INNER JOIN "USFS_FIA"."USFS_FIA"."CONDITION" AS "C"
    ON "A"."plot_sequence_number" = "C"."plot_sequence_number"
    AND "A"."inventory_year" = "C"."inventory_year"
    AND "A"."state_code" = "C"."state_code"
  WHERE "A"."inventory_year" = 2012
    AND "C"."condition_proportion_unadjusted" IS NOT NULL
),
ranked_conditions AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY "evaluation_group" ORDER BY "condition_subplot_acres" DESC) AS rn
  FROM condition_acres
),
top_condition_per_group AS (
  SELECT
    "evaluation_group",
    "evaluation_type",
    "condition_status_code",
    "evaluation_description",
    "state_code",
    "macroplot_acres",
    "condition_subplot_acres"
  FROM ranked_conditions
  WHERE rn = 1
)
SELECT
  "evaluation_group",
  "evaluation_type",
  "condition_status_code",
  "evaluation_description",
  "state_code",
  "macroplot_acres",
  "condition_subplot_acres" AS "subplot_acres"
FROM top_condition_per_group
ORDER BY "condition_subplot_acres" DESC
LIMIT 10