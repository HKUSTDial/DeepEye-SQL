WITH population_data AS (
  SELECT 
    t."GEO_ID",
    EXTRACT(YEAR FROM t."DATE") AS year,
    CAST(t."VALUE" AS FLOAT) AS population,
    REPLACE(t."GEO_ID", 'zip/', '') AS zip_code
  FROM "GLOBAL_GOVERNMENT"."CYBERSYN"."AMERICAN_COMMUNITY_SURVEY_TIMESERIES" t
  WHERE t."VARIABLE" LIKE 'B01003_001E_5YR'
    AND EXTRACT(YEAR FROM t."DATE") BETWEEN 2014 AND 2020
    AND t."GEO_ID" LIKE 'zip/%'
),
zip_state_mapping AS (
  SELECT DISTINCT
    r."RELATED_GEO_ID" AS zip_geo_id,
    SPLIT_PART(i."ISO_3166_2_CODE", '-', 2) AS state_abbr
  FROM "GLOBAL_GOVERNMENT"."CYBERSYN"."GEOGRAPHY_RELATIONSHIPS" r
  JOIN "GLOBAL_GOVERNMENT"."CYBERSYN"."GEOGRAPHY_INDEX" i
    ON r."GEO_ID" = i."GEO_ID"
  WHERE r."RELATIONSHIP_TYPE" = 'Contains'
    AND r."LEVEL" = 'State'
    AND r."RELATED_GEO_ID" LIKE 'zip/%'
),
population_with_state AS (
  SELECT 
    p."GEO_ID",
    p.year,
    p.population,
    p.zip_code,
    z.state_abbr
  FROM population_data p
  LEFT JOIN zip_state_mapping z
    ON p."GEO_ID" = z.zip_geo_id
  WHERE p.population >= 25000
),
population_with_lag AS (
  SELECT 
    "GEO_ID",
    year,
    zip_code,
    state_abbr,
    population,
    LAG(population) OVER (PARTITION BY "GEO_ID" ORDER BY year) AS prev_population
  FROM population_with_state
  WHERE state_abbr IS NOT NULL
),
growth_rates AS (
  SELECT 
    year,
    zip_code,
    state_abbr,
    (population - prev_population) / prev_population * 100 AS growth_rate_pct
  FROM population_with_lag
  WHERE prev_population IS NOT NULL
    AND prev_population > 0
),
ranked_growth AS (
  SELECT 
    year,
    zip_code,
    state_abbr,
    growth_rate_pct,
    RANK() OVER (PARTITION BY year ORDER BY growth_rate_pct DESC) AS rank_pos
  FROM growth_rates
  WHERE year BETWEEN 2015 AND 2020
)
SELECT 
  year,
  zip_code,
  state_abbr,
  growth_rate_pct
FROM ranked_growth
WHERE rank_pos = 2
ORDER BY year