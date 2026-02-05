SELECT
  a.zipcode_geom AS polygon,
  a.area_land_meters,
  a.area_water_meters,
  a.latitude,
  a.longitude,
  a.state_code,
  a.state_name,
  a.city,
  a.county,
  p.total_population
FROM `bigquery-public-data.utility_us.zipcode_area` a
JOIN (
  SELECT
    zipcode,
    SUM(population) AS total_population
  FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010`
  WHERE gender IN ('male', 'female')
  GROUP BY zipcode
) p ON a.zipcode = p.zipcode
WHERE ST_DWITHIN(ST_GEOGFROMTEXT(a.zipcode_geom), ST_GEOGPOINT(-122.3321, 47.6062), 10000)