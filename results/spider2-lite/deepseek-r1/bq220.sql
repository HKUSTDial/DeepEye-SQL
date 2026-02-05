WITH base AS (
  SELECT 
    c.state_code,
    c.state_code_name,
    c.inventory_year AS year,
    c.proportion_basis,
    c.condition_proportion_unadjusted AS P,
    p.expansion_factor AS E,
    p.adjustment_factor_for_the_subplot AS A_s,
    p.adjustment_factor_for_the_macroplot AS A_m
  FROM `bigquery-public-data.usfs_fia.condition` c
  INNER JOIN `bigquery-public-data.usfs_fia.plot_tree` pt 
    ON c.plot_sequence_number = pt.plot_sequence_number
    AND c.state_code = pt.plot_state_code
    AND c.inventory_year = pt.plot_inventory_year
  INNER JOIN `bigquery-public-data.usfs_fia.population_stratum_assign` psa 
    ON c.plot_sequence_number = psa.plot_sequence_number
    AND c.inventory_year = psa.inventory_year
    AND c.state_code = psa.state_code
    AND c.survey_unit_code = psa.survey_unit_code
    AND c.county_code = psa.county_code
    AND c.phase_2_plot_number = psa.phase_2_plot_number
  INNER JOIN `bigquery-public-data.usfs_fia.population` p 
    ON psa.stratum_sequence_number = p.stratum_sequence_number
    AND p.evaluation_type = 'EXPCURR'
    AND c.inventory_year = p.inventory_year
    AND c.state_code = p.state_code
  WHERE c.condition_status_code = 1
    AND c.inventory_year IN (2015, 2016, 2017)
),
size_calc AS (
  SELECT 
    state_code,
    state_code_name,
    year,
    proportion_basis,
    CASE 
      WHEN proportion_basis = 'SUBP' AND A_s > 0 THEN E * P * A_s
      ELSE 0 
    END AS subplot_size,
    CASE 
      WHEN proportion_basis = 'MACR' AND A_m > 0 THEN E * P * A_m
      ELSE 0 
    END AS macroplot_size
  FROM base
),
averages AS (
  SELECT 
    state_code,
    state_code_name,
    year,
    CASE 
      WHEN proportion_basis = 'SUBP' THEN 'subplot'
      WHEN proportion_basis = 'MACR' THEN 'macroplot'
    END AS plot_type,
    AVG(CASE 
      WHEN proportion_basis = 'SUBP' THEN subplot_size
      WHEN proportion_basis = 'MACR' THEN macroplot_size
    END) AS avg_size
  FROM size_calc
  WHERE proportion_basis IN ('SUBP', 'MACR')
  GROUP BY state_code, state_code_name, year, proportion_basis
),
ranked AS (
  SELECT 
    plot_type,
    year,
    state_code_name AS state,
    avg_size,
    RANK() OVER (PARTITION BY year, plot_type ORDER BY avg_size DESC) AS rnk
  FROM averages
)
SELECT 
  plot_type,
  year,
  state,
  avg_size
FROM ranked
WHERE rnk = 1
ORDER BY year, plot_type