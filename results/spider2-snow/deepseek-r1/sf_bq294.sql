WITH filtered_trips AS (
  SELECT 
    "trip_id",
    "duration_sec",
    "start_date",
    "start_station_name",
    "end_station_name",
    "bike_number",
    "subscriber_type",
    "member_birth_year",
    "member_gender",
    "start_station_id"
  FROM "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_TRIPS"
  WHERE 
    "start_station_name" IS NOT NULL
    AND "member_birth_year" IS NOT NULL
    AND "member_gender" IS NOT NULL
    AND TO_DATE(TO_TIMESTAMP("start_date" / 1000000)) BETWEEN DATE '2017-07-01' AND DATE '2017-12-31'
),
trips_with_age AS (
  SELECT 
    *,
    EXTRACT(YEAR FROM CURRENT_DATE()) - CAST("member_birth_year" AS INTEGER) AS age
  FROM filtered_trips
),
trips_with_class AS (
  SELECT 
    *,
    CASE 
      WHEN age < 40 THEN 'Young (<40 Y.O)'
      WHEN age BETWEEN 40 AND 60 THEN 'Adult (40-60 Y.O)'
      ELSE 'Senior Adult (>60 Y.O)'
    END AS age_class,
    "start_station_name" || ' - ' || "end_station_name" AS route
  FROM trips_with_age
)
SELECT 
  t."trip_id",
  t."duration_sec",
  TO_CHAR(TO_TIMESTAMP(t."start_date" / 1000000), 'YYYY-MM-DD HH24:MI:SS') AS start_date,
  t."start_station_name",
  t.route,
  t."bike_number",
  t."subscriber_type",
  CAST(t."member_birth_year" AS INTEGER) AS member_birth_year,
  t.age,
  t.age_class,
  t."member_gender",
  r."name" AS region_name
FROM trips_with_class t
LEFT JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_STATION_INFO" si
  ON CAST(t."start_station_id" AS VARCHAR) = si."station_id"
LEFT JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_REGIONS" r
  ON si."region_id" = r."region_id"
ORDER BY t."duration_sec" DESC
LIMIT 5