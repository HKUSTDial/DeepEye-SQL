WITH filtered_trips AS (
  SELECT 
    "trip_id",
    "start_station_id",
    "start_station_name",
    "duration_sec",
    "member_gender",
    "start_date",
    TO_TIMESTAMP("start_date" / 1000000) AS start_timestamp
  FROM 
    "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_TRIPS"
  WHERE 
    EXTRACT(YEAR FROM TO_TIMESTAMP("start_date" / 1000000)) BETWEEN 2014 AND 2017
),
joined_trips AS (
  SELECT 
    t.*,
    s."region_id",
    r."name" AS region_name
  FROM 
    filtered_trips t
    JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_STATION_INFO" s 
      ON CAST(t."start_station_id" AS TEXT) = s."station_id"
    JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_REGIONS" r 
      ON s."region_id" = r."region_id"
),
ranked_trips AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY "region_id" ORDER BY "start_date" DESC) AS rn
  FROM 
    joined_trips
)
SELECT 
  region_name,
  "trip_id",
  "duration_sec" AS ride_duration,
  start_timestamp AS start_time,
  "start_station_name" AS starting_station,
  "member_gender" AS gender
FROM 
  ranked_trips
WHERE 
  rn = 1