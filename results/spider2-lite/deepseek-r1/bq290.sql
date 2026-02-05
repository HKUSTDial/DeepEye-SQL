SELECT 
  us.date,
  us.us_max - uk.uk_max AS max_temp_diff,
  us.us_min - uk.uk_min AS min_temp_diff,
  us.us_temp - uk.uk_temp AS avg_temp_diff
FROM (
  SELECT 
    g.date,
    AVG(g.max) AS us_max,
    AVG(g.min) AS us_min,
    AVG(g.temp) AS us_temp
  FROM 
    `bigquery-public-data.noaa_gsod.gsod2023` g
  JOIN 
    `bigquery-public-data.noaa_gsod.stations` s 
    ON g.stn = s.usaf AND g.wban = s.wban
  WHERE 
    s.country = 'US'
    AND EXTRACT(YEAR FROM g.date) = 2023
    AND EXTRACT(MONTH FROM g.date) = 10
    AND g.max != 9999.9
    AND g.min != 9999.9
    AND g.temp != 9999.9
  GROUP BY 
    g.date
) us
JOIN (
  SELECT 
    g.date,
    AVG(g.max) AS uk_max,
    AVG(g.min) AS uk_min,
    AVG(g.temp) AS uk_temp
  FROM 
    `bigquery-public-data.noaa_gsod.gsod2023` g
  JOIN 
    `bigquery-public-data.noaa_gsod.stations` s 
    ON g.stn = s.usaf AND g.wban = s.wban
  WHERE 
    s.country = 'UK'
    AND EXTRACT(YEAR FROM g.date) = 2023
    AND EXTRACT(MONTH FROM g.date) = 10
    AND g.max != 9999.9
    AND g.min != 9999.9
    AND g.temp != 9999.9
  GROUP BY 
    g.date
) uk
ON us.date = uk.date
ORDER BY 
  us.date