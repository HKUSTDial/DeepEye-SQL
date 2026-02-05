SELECT 
  COALESCE(corn.state_name, mushroom.state_name) AS state_name,
  corn.corn_total,
  mushroom.mushroom_total
FROM (
  SELECT state_name, SUM(value) AS corn_total
  FROM `bigquery-public-data.usda_nass_agriculture.crops`
  WHERE year = 2022
    AND commodity_desc = 'CORN'
    AND group_desc = 'FIELD CROPS'
    AND statisticcat_desc = 'PRODUCTION'
    AND agg_level_desc = 'STATE'
    AND unit_desc = 'BU'
    AND value IS NOT NULL
    AND state_fips_code NOT IN (98, 99)
  GROUP BY state_name
) AS corn
FULL OUTER JOIN (
  SELECT state_name, SUM(value) AS mushroom_total
  FROM `bigquery-public-data.usda_nass_agriculture.crops`
  WHERE year = 2022
    AND commodity_desc = 'MUSHROOMS'
    AND group_desc = 'HORTICULTURE'
    AND statisticcat_desc = 'PRODUCTION'
    AND agg_level_desc = 'STATE'
    AND value IS NOT NULL
    AND state_fips_code NOT IN (98, 99)
  GROUP BY state_name
) AS mushroom
ON corn.state_name = mushroom.state_name