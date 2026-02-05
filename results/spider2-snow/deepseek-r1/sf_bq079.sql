WITH latest_eval_group AS (
  SELECT "state_code", "evaluation_group"
  FROM (
    SELECT "state_code", "evaluation_group", ROW_NUMBER() OVER (PARTITION BY "state_code" ORDER BY CAST("end_inventory_year" AS INTEGER) DESC, "evaluation_group" DESC) AS rn
    FROM (
      SELECT DISTINCT "state_code", "evaluation_group", "end_inventory_year"
      FROM "USFS_FIA"."USFS_FIA"."POPULATION"
      WHERE "evaluation_type" = 'EXPCURR'
    ) t
  )
  WHERE rn = 1
),
state_names AS (
  SELECT leg."state_code", leg."evaluation_group", MAX(pe."location_name") AS state_name
  FROM latest_eval_group leg
  INNER JOIN "USFS_FIA"."USFS_FIA"."POPULATION_EVALUATION_GROUP" peg ON leg."evaluation_group" = peg."evaluation_group" AND leg."state_code" = peg."state_code"
  INNER JOIN "USFS_FIA"."USFS_FIA"."POPULATION_EVALUATION" pe ON peg."evaluation_group_sequence_number" = pe."evaluation_group_sequence_number"
  WHERE pe."state_code" = leg."state_code"
  GROUP BY leg."state_code", leg."evaluation_group"
),
timberland_acres AS (
  SELECT p."state_code", p."evaluation_group", sn.state_name, SUM(p."expansion_factor" * c."condition_proportion_unadjusted" * CASE WHEN c."proportion_basis" = 'MACR' AND p."adjustment_factor_for_the_macroplot" > 0 THEN p."adjustment_factor_for_the_macroplot" WHEN c."proportion_basis" = 'SUBP' AND p."adjustment_factor_for_the_subplot" > 0 THEN p."adjustment_factor_for_the_subplot" ELSE 0 END) AS total_acres
  FROM "USFS_FIA"."USFS_FIA"."POPULATION" p
  INNER JOIN latest_eval_group leg ON p."state_code" = leg."state_code" AND p."evaluation_group" = leg."evaluation_group"
  INNER JOIN state_names sn ON p."state_code" = sn."state_code" AND p."evaluation_group" = sn."evaluation_group"
  INNER JOIN "USFS_FIA"."USFS_FIA"."CONDITION" c ON p."plot_sequence_number" = c."plot_sequence_number" AND p."state_code" = c."state_code"
  WHERE p."evaluation_type" = 'EXPCURR' AND c."condition_status_code" = 1 AND c."reserved_status_code" = 0 AND c."site_productivity_class_code" BETWEEN 1 AND 6 AND c."proportion_basis" IN ('MACR', 'SUBP') AND c."condition_proportion_unadjusted" IS NOT NULL AND ((c."proportion_basis" = 'MACR' AND p."adjustment_factor_for_the_macroplot" > 0) OR (c."proportion_basis" = 'SUBP' AND p."adjustment_factor_for_the_subplot" > 0))
  GROUP BY p."state_code", p."evaluation_group", sn.state_name
),
forestland_acres AS (
  SELECT p."state_code", p."evaluation_group", sn.state_name, SUM(p."expansion_factor" * c."condition_proportion_unadjusted" * CASE WHEN c."proportion_basis" = 'MACR' AND p."adjustment_factor_for_the_macroplot" > 0 THEN p."adjustment_factor_for_the_macroplot" WHEN c."proportion_basis" = 'SUBP' AND p."adjustment_factor_for_the_subplot" > 0 THEN p."adjustment_factor_for_the_subplot" ELSE 0 END) AS total_acres
  FROM "USFS_FIA"."USFS_FIA"."POPULATION" p
  INNER JOIN latest_eval_group leg ON p."state_code" = leg."state_code" AND p."evaluation_group" = leg."evaluation_group"
  INNER JOIN state_names sn ON p."state_code" = sn."state_code" AND p."evaluation_group" = sn."evaluation_group"
  INNER JOIN "USFS_FIA"."USFS_FIA"."CONDITION" c ON p."plot_sequence_number" = c."plot_sequence_number" AND p."state_code" = c."state_code"
  WHERE p."evaluation_type" = 'EXPCURR' AND c."condition_status_code" = 1 AND c."proportion_basis" IN ('MACR', 'SUBP') AND c."condition_proportion_unadjusted" IS NOT NULL AND ((c."proportion_basis" = 'MACR' AND p."adjustment_factor_for_the_macroplot" > 0) OR (c."proportion_basis" = 'SUBP' AND p."adjustment_factor_for_the_subplot" > 0))
  GROUP BY p."state_code", p."evaluation_group", sn.state_name
)
(SELECT 'timberland' AS category, "state_code", "evaluation_group", state_name, total_acres FROM timberland_acres ORDER BY total_acres DESC LIMIT 1)
UNION ALL
(SELECT 'forestland' AS category, "state_code", "evaluation_group", state_name, total_acres FROM forestland_acres ORDER BY total_acres DESC LIMIT 1)