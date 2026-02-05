WITH years AS (
  SELECT CAST(year_str AS STRING) AS year
  FROM UNNEST(['2011','2012','2013','2014','2015','2016','2017','2018','2019','2020']) AS year_str
),
data_2011_2019 AS (
  SELECT 
    `year`,
    CASE WHEN `temp` = 9999.9 THEN NULL ELSE `temp` END AS avg_temp,
    CASE WHEN `wdsp` = '999.9' THEN NULL ELSE CAST(`wdsp` AS FLOAT64) END AS avg_wind_speed,
    CASE WHEN `prcp` = 99.99 THEN NULL ELSE `prcp` END AS precipitation
  FROM `bigquery-public-data.noaa_gsod.gsod20*`
  WHERE _TABLE_SUFFIX BETWEEN '11' AND '19'
    AND `stn` = '725030'
    AND `mo` = '06'
    AND `da` = '12'
),
data_2020 AS (
  SELECT 
    `year`,
    CASE WHEN `temp` = 9999.9 THEN NULL ELSE `temp` END AS avg_temp,
    CASE WHEN `wdsp` = '999.9' THEN NULL ELSE CAST(`wdsp` AS FLOAT64) END AS avg_wind_speed,
    CASE WHEN `prcp` = 99.99 THEN NULL ELSE `prcp` END AS precipitation
  FROM `bigquery-public-data.noaa_gsod.gsod2020`
  WHERE `stn` = '725030'
    AND `mo` = '06'
    AND `da` = '12'
),
all_data AS (
  SELECT * FROM data_2011_2019
  UNION ALL
  SELECT * FROM data_2020
)
SELECT 
  y.`year`,
  d.avg_temp,
  d.avg_wind_speed,
  d.precipitation
FROM years y
LEFT JOIN all_data d ON y.`year` = d.`year`
ORDER BY y.`year`