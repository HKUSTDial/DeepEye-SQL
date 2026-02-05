WITH nearby_stations AS (
  SELECT 
    `usaf`,
    `wban`,
    `name`,
    `lat`,
    `lon`
  FROM `bigquery-public-data.noaa_gsod.stations`
  WHERE ST_DWITHIN(
    ST_GeogPoint(`lon`, `lat`),
    ST_GeogPoint(-73.764, 41.197),
    32186.9
  )
)
SELECT 
  ns.`name`,
  ns.`lat`,
  ns.`lon`,
  SUM(g.`count_temp`) AS total_valid_observations
FROM nearby_stations ns
JOIN `bigquery-public-data.noaa_gsod.gsod*` g
  ON ns.`usaf` = g.`stn` 
  AND ns.`wban` = g.`wban`
WHERE 
  _TABLE_SUFFIX BETWEEN '2011' AND '2020'
  AND g.`temp` != 9999.9
GROUP BY 
  ns.`name`,
  ns.`lat`,
  ns.`lon`
ORDER BY ns.`name`