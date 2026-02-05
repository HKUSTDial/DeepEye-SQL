WITH wa_stations AS (
  SELECT usaf, wban, name
  FROM `bigquery-public-data.noaa_gsod.stations`
  WHERE state = 'WA'
),
rainy_2023 AS (
  SELECT s.usaf, s.wban, s.name, COUNT(*) AS cnt_2023
  FROM `bigquery-public-data.noaa_gsod.gsod2023` g
  INNER JOIN wa_stations s ON g.stn = s.usaf AND g.wban = s.wban
  WHERE EXTRACT(YEAR FROM g.date) = 2023 AND g.prcp > 0 AND g.prcp != 99.99 AND g.prcp IS NOT NULL
  GROUP BY s.usaf, s.wban, s.name
),
rainy_2022 AS (
  SELECT s.usaf, s.wban, s.name, COUNT(*) AS cnt_2022
  FROM `bigquery-public-data.noaa_gsod.gsod2022` g
  INNER JOIN wa_stations s ON g.stn = s.usaf AND g.wban = s.wban
  WHERE EXTRACT(YEAR FROM g.date) = 2022 AND g.prcp > 0 AND g.prcp != 99.99 AND g.prcp IS NOT NULL
  GROUP BY s.usaf, s.wban, s.name
)
SELECT r23.usaf, r23.wban, r23.name, r23.cnt_2023, r22.cnt_2022
FROM rainy_2023 r23
INNER JOIN rainy_2022 r22 ON r23.usaf = r22.usaf AND r23.wban = r22.wban
WHERE r23.cnt_2023 > 150 AND r23.cnt_2023 < r22.cnt_2022