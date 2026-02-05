WITH population_data AS (
  SELECT 
    ts."GEO_ID" AS zip_geo_id,
    EXTRACT(YEAR FROM ts."DATE") AS year,
    AVG(TRY_CAST(ts."VALUE" AS FLOAT)) AS population
  FROM "GLOBAL_GOVERNMENT"."CYBERSYN"."AMERICAN_COMMUNITY_SURVEY_TIMESERIES" ts
  INNER JOIN "GLOBAL_GOVERNMENT"."CYBERSYN"."AMERICAN_COMMUNITY_SURVEY_ATTRIBUTES" att 
    ON ts."VARIABLE" = att."VARIABLE"
  WHERE att."MEASURE" = 'Population'
    AND att."MEASUREMENT_PERIOD" = '5YR'
    AND att."MEASUREMENT_TYPE" = 'Estimate'
    AND att."RACE" = 'NULL'
    AND EXTRACT(YEAR FROM ts."DATE") BETWEEN 2014 AND 2020
    AND EXISTS (
      SELECT 1 
      FROM "GLOBAL_GOVERNMENT"."CYBERSYN"."GEOGRAPHY_RELATIONSHIPS" gr
      WHERE gr."RELATED_GEO_ID" = ts."GEO_ID"
        AND gr."RELATED_LEVEL" = 'CensusZipCodeTabulationArea'
        AND gr."RELATIONSHIP_TYPE" = 'Contains'
        AND gr."LEVEL" = 'State'
    )
  GROUP BY ts."GEO_ID", EXTRACT(YEAR FROM ts."DATE")
  HAVING AVG(TRY_CAST(ts."VALUE" AS FLOAT)) >= 25000
),
state_info AS (
  SELECT DISTINCT
    gr."RELATED_GEO_ID" AS zip_geo_id,
    SPLIT_PART(gi."ISO_3166_2_CODE", '-', 2) AS state_abbr
  FROM "GLOBAL_GOVERNMENT"."CYBERSYN"."GEOGRAPHY_RELATIONSHIPS" gr
  INNER JOIN "GLOBAL_GOVERNMENT"."CYBERSYN"."GEOGRAPHY_INDEX" gi 
    ON gr."GEO_ID" = gi."GEO_ID"
  WHERE gr."RELATIONSHIP_TYPE" = 'Contains'
    AND gr."RELATED_LEVEL" = 'CensusZipCodeTabulationArea'
    AND gr."LEVEL" = 'State'
),
growth_calc AS (
  SELECT 
    p1.zip_geo_id,
    p1.year,
    p1.population,
    si.state_abbr,
    ((p1.population - p2.population) / NULLIF(p2.population, 0)) * 100 AS growth_rate_pct
  FROM population_data p1
  INNER JOIN population_data p2 
    ON p1.zip_geo_id = p2.zip_geo_id 
    AND p1.year = p2.year + 1
  INNER JOIN state_info si ON p1.zip_geo_id = si.zip_geo_id
  WHERE p1.year BETWEEN 2015 AND 2020
),
ranked_growth AS (
  SELECT 
    year,
    zip_geo_id,
    state_abbr,
    growth_rate_pct,
    DENSE_RANK() OVER (PARTITION BY year ORDER BY growth_rate_pct DESC) AS rank_num
  FROM growth_calc
  WHERE growth_rate_pct IS NOT NULL
)
SELECT 
  year,
  SPLIT_PART(zip_geo_id, '/', 2) AS zip_code,
  state_abbr,
  ROUND(growth_rate_pct, 2) AS annual_growth_rate_percentage
FROM ranked_growth
WHERE rank_num = 2
ORDER BY year