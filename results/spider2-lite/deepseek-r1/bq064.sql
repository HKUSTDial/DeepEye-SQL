WITH tract_data AS (
  SELECT 
    c.geo_id,
    c.total_pop,
    c.income_per_capita,
    g.tract_geom
  FROM `bigquery-public-data.census_bureau_acs.censustract_2017_5yr` c
  JOIN `bigquery-public-data.geo_census_tracts.us_census_tracts_national` g
    ON c.geo_id = g.geo_id
),
zip_data AS (
  SELECT 
    zip_code,
    zip_code_geom
  FROM `bigquery-public-data.geo_us_boundaries.zip_codes`
  WHERE ST_DWITHIN(
    zip_code_geom,
    ST_GEOGPOINT(-122.191667, 47.685833),
    8046.72
  )
),
tract_zip_intersections AS (
  SELECT 
    t.geo_id,
    t.total_pop,
    t.income_per_capita,
    z.zip_code,
    ST_AREA(t.tract_geom) AS tract_area,
    ST_AREA(ST_INTERSECTION(t.tract_geom, z.zip_code_geom)) AS intersection_area
  FROM tract_data t
  JOIN zip_data z ON ST_INTERSECTS(t.tract_geom, z.zip_code_geom)
  WHERE ST_AREA(t.tract_geom) > 0
),
allocated_data AS (
  SELECT 
    zip_code,
    total_pop * (intersection_area / tract_area) AS allocated_population,
    income_per_capita * total_pop * (intersection_area / tract_area) AS allocated_income
  FROM tract_zip_intersections
  WHERE tract_area > 0
),
zip_totals AS (
  SELECT 
    zip_code,
    SUM(allocated_population) AS total_population,
    SUM(allocated_income) AS total_income
  FROM allocated_data
  GROUP BY zip_code
  HAVING SUM(allocated_population) > 0
)
SELECT 
  zip_code,
  ROUND(total_population, 1) AS total_population,
  ROUND(total_income / total_population, 1) AS average_individual_income
FROM zip_totals
ORDER BY average_individual_income DESC