SELECT
  EXTRACT(YEAR FROM `date`) AS `year`,
  MAX(CASE WHEN `element` = 'PRCP' THEN `value` END) AS `max_precipitation_mm`,
  MIN(CASE WHEN `element` = 'TMIN' THEN `value` END) AS `min_temperature_c`,
  MAX(CASE WHEN `element` = 'TMAX' THEN `value` END) AS `max_temperature_c`
FROM `bigquery-public-data.ghcn_d.ghcnd_20*`
WHERE
  _TABLE_SUFFIX BETWEEN '13' AND '16'
  AND `id` = 'USW00094846'
  AND (`qflag` IS NULL OR `qflag` = '')
  AND `value` IS NOT NULL
  AND EXTRACT(MONTH FROM `date`) = 12
  AND EXTRACT(DAY FROM `date`) >= 17
  AND `element` IN ('PRCP', 'TMIN', 'TMAX')
GROUP BY `year`
ORDER BY `year`