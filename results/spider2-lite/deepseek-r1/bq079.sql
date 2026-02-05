WITH latest_eval_group AS (
  SELECT 
    peg.state_code,
    peg.evaluation_group,
    peg.pop_evaluation_group_created_date,
    ROW_NUMBER() OVER (PARTITION BY peg.state_code ORDER BY peg.pop_evaluation_group_created_date DESC) AS rn
  FROM `bigquery-public-data.usfs_fia.population_evaluation_group` peg
  INNER JOIN `bigquery-public-data.usfs_fia.population_evaluation_type` pet
    ON peg.evaluation_group_sequence_number = pet.evaluation_group_sequence_number
  WHERE pet.evaluation_type = 'EXPCURR'
),
filtered_latest_eval_group AS (
  SELECT state_code, evaluation_group
  FROM latest_eval_group
  WHERE rn = 1
),
condition_acres AS (
  SELECT 
    p.state_code,
    c.state_code_name,
    p.evaluation_group,
    c.condition_status_code,
    c.reserved_status_code,
    c.site_productivity_class_code,
    c.proportion_basis,
    c.condition_proportion_unadjusted,
    p.expansion_factor,
    p.adjustment_factor_for_the_subplot,
    p.adjustment_factor_for_the_macroplot,
    CASE 
      WHEN c.proportion_basis = 'MACR' AND p.adjustment_factor_for_the_macroplot > 0 THEN p.expansion_factor * c.condition_proportion_unadjusted * p.adjustment_factor_for_the_macroplot
      WHEN c.proportion_basis = 'SUBP' AND p.adjustment_factor_for_the_subplot > 0 THEN p.expansion_factor * c.condition_proportion_unadjusted * p.adjustment_factor_for_the_subplot
      ELSE p.expansion_factor * c.condition_proportion_unadjusted
    END AS adjusted_acres
  FROM `bigquery-public-data.usfs_fia.population` p
  INNER JOIN filtered_latest_eval_group leg 
    ON p.state_code = leg.state_code AND p.evaluation_group = leg.evaluation_group
  INNER JOIN `bigquery-public-data.usfs_fia.condition` c
    ON p.plot_sequence_number = c.plot_sequence_number
    AND p.state_code = c.state_code
    AND p.inventory_year = c.inventory_year
    AND p.survey_unit_code = c.survey_unit_code
    AND p.county_code = c.county_code
    AND p.phase_2_plot_number = c.phase_2_plot_number
  WHERE p.evaluation_type = 'EXPCURR'
),
state_totals AS (
  SELECT 
    state_code,
    state_code_name,
    evaluation_group,
    SUM(CASE 
          WHEN condition_status_code = 1 
               AND reserved_status_code = 0 
               AND site_productivity_class_code BETWEEN 1 AND 6 
          THEN adjusted_acres 
          ELSE 0 
        END) AS timberland_acres,
    SUM(CASE 
          WHEN condition_status_code = 1 
          THEN adjusted_acres 
          ELSE 0 
        END) AS forestland_acres
  FROM condition_acres
  GROUP BY state_code, state_code_name, evaluation_group
),
ranked_timber AS (
  SELECT 
    'timberland' AS category,
    state_code,
    evaluation_group,
    state_code_name,
    timberland_acres AS total_acres,
    RANK() OVER (ORDER BY timberland_acres DESC) AS rnk
  FROM state_totals
  WHERE timberland_acres > 0
),
ranked_forest AS (
  SELECT 
    'forestland' AS category,
    state_code,
    evaluation_group,
    state_code_name,
    forestland_acres AS total_acres,
    RANK() OVER (ORDER BY forestland_acres DESC) AS rnk
  FROM state_totals
  WHERE forestland_acres > 0
)
SELECT category, state_code, evaluation_group, state_code_name, total_acres
FROM ranked_timber
WHERE rnk = 1
UNION ALL
SELECT category, state_code, evaluation_group, state_code_name, total_acres
FROM ranked_forest
WHERE rnk = 1