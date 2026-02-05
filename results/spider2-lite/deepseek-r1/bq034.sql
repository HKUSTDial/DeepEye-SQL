SELECT `id`, `name`
FROM `bigquery-public-data.ghcn_d.ghcnd_stations`
WHERE ST_DISTANCE(
  ST_GEOGPOINT(`longitude`, `latitude`),
  ST_GEOGPOINT(-87.6847, 41.8319)
) <= 50000