WITH census_1995 AS (
  SELECT 
    UPPER(spc_latin) as latin_name,
    spc_common as common_name,
    COUNT(*) as total_1995,
    SUM(CASE WHEN status IN ('Good', 'Poor') THEN 1 ELSE 0 END) as alive_1995,
    SUM(CASE WHEN status = 'Dead' THEN 1 ELSE 0 END) as dead_1995
  FROM `bigquery-public-data.new_york.tree_census_1995`
  WHERE spc_latin IS NOT NULL AND spc_latin != ''
  GROUP BY 1, 2
),
census_2005 AS (
  SELECT 
    UPPER(spc_latin) as latin_name,
    spc_common as common_name,
    COUNT(*) as total_2005,
    SUM(CASE WHEN status IN ('Excellent', 'Good', 'Poor') THEN 1 ELSE 0 END) as alive_2005,
    SUM(CASE WHEN status = 'Dead' THEN 1 ELSE 0 END) as dead_2005
  FROM `bigquery-public-data.new_york.tree_census_2005`
  WHERE spc_latin IS NOT NULL AND spc_latin != ''
  GROUP BY 1, 2
),
census_2015 AS (
  SELECT 
    UPPER(spc_latin) as latin_name,
    spc_common as common_name,
    COUNT(*) as total_2015,
    SUM(CASE WHEN status = 'Alive' THEN 1 ELSE 0 END) as alive_2015,
    SUM(CASE WHEN status = 'Dead' THEN 1 ELSE 0 END) as dead_2015
  FROM `bigquery-public-data.new_york.tree_census_2015`
  WHERE spc_latin IS NOT NULL AND spc_latin != ''
  GROUP BY 1, 2
),
combined AS (
  SELECT 
    COALESCE(t1.latin_name, t2.latin_name, t3.latin_name) as latin_name,
    COALESCE(t3.common_name, t2.common_name, t1.common_name) as common_name,
    IFNULL(t1.total_1995, 0) as total_1995,
    IFNULL(t1.alive_1995, 0) as alive_1995,
    IFNULL(t1.dead_1995, 0) as dead_1995,
    IFNULL(t2.total_2005, 0) as total_2005,
    IFNULL(t2.alive_2005, 0) as alive_2005,
    IFNULL(t2.dead_2005, 0) as dead_2005,
    IFNULL(t3.total_2015, 0) as total_2015,
    IFNULL(t3.alive_2015, 0) as alive_2015,
    IFNULL(t3.dead_2015, 0) as dead_2015
  FROM census_1995 t1
  FULL OUTER JOIN census_2005 t2 ON t1.latin_name = t2.latin_name
  FULL OUTER JOIN census_2015 t3 ON COALESCE(t1.latin_name, t2.latin_name) = t3.latin_name
)
SELECT 
  latin_name,
  common_name,
  total_1995,
  alive_1995,
  dead_1995,
  total_2005,
  alive_2005,
  dead_2005,
  total_2015,
  alive_2015,
  dead_2015,
  total_2015 - total_1995 as total_growth,
  alive_2015 - alive_1995 as alive_growth,
  dead_2015 - dead_1995 as dead_growth
FROM combined
WHERE latin_name IS NOT NULL
ORDER BY total_growth DESC
LIMIT 10