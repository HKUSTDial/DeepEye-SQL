WITH clay_stop AS (
  SELECT stop_id FROM `bigquery-public-data.san_francisco_transit_muni.stops`
  WHERE stop_name = 'Clay St & Drumm St'
),
sac_stop AS (
  SELECT stop_id FROM `bigquery-public-data.san_francisco_transit_muni.stops`
  WHERE stop_name = 'Sacramento St & Davis St'
),
clay_times AS (
  SELECT 
    CAST(st.trip_id AS STRING) AS trip_id,
    st.stop_sequence,
    st.departure_time
  FROM `bigquery-public-data.san_francisco_transit_muni.stop_times` st
  INNER JOIN clay_stop c ON CAST(st.stop_id AS STRING) = c.stop_id
),
sac_times AS (
  SELECT 
    CAST(st.trip_id AS STRING) AS trip_id,
    st.stop_sequence,
    st.arrival_time
  FROM `bigquery-public-data.san_francisco_transit_muni.stop_times` st
  INNER JOIN sac_stop s ON CAST(st.stop_id AS STRING) = s.stop_id
),
valid_trips AS (
  SELECT
    clay.trip_id,
    clay.departure_time AS clay_departure,
    sac.arrival_time AS sac_arrival
  FROM clay_times clay
  INNER JOIN sac_times sac ON clay.trip_id = sac.trip_id
  WHERE clay.stop_sequence < sac.stop_sequence
)
SELECT
  t.route_id,
  t.trip_headsign,
  FORMAT_TIME('%T', MIN(v.clay_departure)) AS earliest_departure,
  FORMAT_TIME('%T', MAX(v.sac_arrival)) AS latest_arrival
FROM valid_trips v
INNER JOIN `bigquery-public-data.san_francisco_transit_muni.trips` t ON v.trip_id = t.trip_id
GROUP BY t.route_id, t.trip_headsign
ORDER BY t.route_id, t.trip_headsign