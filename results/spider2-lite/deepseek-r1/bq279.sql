WITH station_years AS (
  SELECT
    EXTRACT(YEAR FROM start_time) AS year,
    CAST(start_station_id AS INT64) AS station_id
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  WHERE EXTRACT(YEAR FROM start_time) IN (2013, 2014)
  UNION DISTINCT
  SELECT
    EXTRACT(YEAR FROM start_time) AS year,
    CAST(end_station_id AS INT64) AS station_id
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  WHERE EXTRACT(YEAR FROM start_time) IN (2013, 2014)
),
latest_stations AS (
  SELECT
    station_id,
    status,
    ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY modified_date DESC) AS rn
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations`
)
SELECT
  sy.year,
  COUNT(DISTINCT IF(st.status = 'active', sy.station_id, NULL)) AS active_count,
  COUNT(DISTINCT IF(st.status = 'closed', sy.station_id, NULL)) AS closed_count
FROM station_years sy
LEFT JOIN latest_stations st ON sy.station_id = st.station_id AND st.rn = 1
GROUP BY sy.year
ORDER BY sy.year