WITH condition_acres AS (
  SELECT 
    c.condition_status_code,
    p.evaluation_group,
    p.evaluation_type,
    p.evaluation_description,
    p.state_code,
    c.subplot_proportion_unadjusted * p.expansion_factor * p.adjustment_factor_for_the_subplot AS subplot_acres,
    c.macroplot_proportion_unadjusted * p.expansion_factor * p.adjustment_factor_for_the_macroplot AS macroplot_acres
  FROM `bigquery-public-data.usfs_fia.condition` c
  JOIN `bigquery-public-data.usfs_fia.population` p
    ON c.plot_sequence_number = p.plot_sequence_number
    AND c.state_code = p.state_code
    AND c.inventory_year = p.inventory_year
  WHERE c.inventory_year = 2012
    AND c.subplot_proportion_unadjusted IS NOT NULL
    AND p.adjustment_factor_for_the_subplot IS NOT NULL
    AND p.expansion_factor IS NOT NULL
),
ranked_conditions AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY evaluation_group ORDER BY subplot_acres DESC) AS rn
  FROM condition_acres
)
SELECT 
  evaluation_group,
  evaluation_type,
  condition_status_code,
  evaluation_description,
  state_code,
  macroplot_acres,
  subplot_acres
FROM ranked_conditions
WHERE rn = 1
ORDER BY subplot_acres DESC
LIMIT 10