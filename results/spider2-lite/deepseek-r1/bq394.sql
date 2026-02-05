SELECT
  year,
  month,
  (ABS(AVG(air_temperature) - AVG(wetbulb_temperature)) +
   ABS(AVG(air_temperature) - AVG(dewpoint_temperature)) +
   ABS(AVG(air_temperature) - AVG(sea_surface_temp)) +
   ABS(AVG(wetbulb_temperature) - AVG(dewpoint_temperature)) +
   ABS(AVG(wetbulb_temperature) - AVG(sea_surface_temp)) +
   ABS(AVG(dewpoint_temperature) - AVG(sea_surface_temp))
  ) AS sum_abs_diff
FROM `bigquery-public-data.noaa_icoads.icoads_core_201*`
WHERE _TABLE_SUFFIX IN ('0','1','2','3','4')
  AND year BETWEEN 2010 AND 2014
  AND air_temperature IS NOT NULL
  AND wetbulb_temperature IS NOT NULL
  AND dewpoint_temperature IS NOT NULL
  AND sea_surface_temp IS NOT NULL
GROUP BY year, month
ORDER BY sum_abs_diff ASC
LIMIT 3