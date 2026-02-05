WITH all_zip_codes AS (
  SELECT DISTINCT "zip_code"
  FROM "NEW_YORK_GEO"."GEO_US_BOUNDARIES"."ZIP_CODES"
  WHERE "state_name" = 'New York'
    AND ("city" LIKE '%New York%' OR "city" LIKE '%Brooklyn%' OR "city" LIKE '%Queens%' 
         OR "city" LIKE '%Bronx%' OR "city" LIKE '%Staten Island%' OR "city" LIKE '%Manhattan%')
),
all_hours AS (
  SELECT seq4() as hour FROM TABLE(GENERATOR(ROWCOUNT => 24))
),
date_range AS (
  SELECT DATEADD(day, seq4(), '2014-12-18'::DATE) as trip_date
  FROM TABLE(GENERATOR(ROWCOUNT => 15))
  UNION ALL
  SELECT '2015-01-01'::DATE as trip_date
),
hourly_trips_base AS (
  SELECT 
    z."zip_code",
    DATE(TIMESTAMP_MICROS(t."pickup_datetime"/1000)) as trip_date,
    EXTRACT(HOUR FROM TIMESTAMP_MICROS(t."pickup_datetime"/1000)) as hour,
    COUNT(*) as trip_count
  FROM "NEW_YORK_GEO"."NEW_YORK"."TLC_YELLOW_TRIPS_2015" t
  JOIN "NEW_YORK_GEO"."GEO_US_BOUNDARIES"."ZIP_CODES" z
    ON ST_CONTAINS(z."zip_code_geom", 
        ST_GEOGPOINT(t."pickup_longitude", t."pickup_latitude"))
  WHERE t."pickup_datetime" BETWEEN 1418860800000000 AND 1420156799999999
    AND t."pickup_latitude" IS NOT NULL
    AND t."pickup_longitude" IS NOT NULL
    AND DATE(TIMESTAMP_MICROS(t."pickup_datetime"/1000)) BETWEEN '2014-12-18' AND '2015-01-01'
  GROUP BY z."zip_code", trip_date, hour
),
all_combinations AS (
  SELECT 
    z."zip_code",
    h.hour,
    d.trip_date
  FROM all_zip_codes z
  CROSS JOIN all_hours h
  CROSS JOIN date_range d
),
hourly_trips_complete AS (
  SELECT 
    ac."zip_code",
    ac.trip_date,
    ac.hour,
    COALESCE(ht.trip_count, 0) as trip_count
  FROM all_combinations ac
  LEFT JOIN hourly_trips_base ht
    ON ac."zip_code" = ht."zip_code"
    AND ac.trip_date = ht.trip_date
    AND ac.hour = ht.hour
),
hourly_with_stats AS (
  SELECT 
    "zip_code",
    trip_date,
    hour,
    trip_count,
    LAG(trip_count, 1) OVER (PARTITION BY "zip_code" ORDER BY trip_date, hour) as trips_1h_ago,
    LAG(trip_count, 24) OVER (PARTITION BY "zip_code" ORDER BY trip_date, hour) as trips_24h_ago,
    LAG(trip_count, 168) OVER (PARTITION BY "zip_code" ORDER BY trip_date, hour) as trips_168h_ago,
    LAG(trip_count, 336) OVER (PARTITION BY "zip_code" ORDER BY trip_date, hour) as trips_336h_ago,
    AVG(trip_count) OVER (PARTITION BY "zip_code", hour ORDER BY trip_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) as moving_avg_14d,
    STDDEV(trip_count) OVER (PARTITION BY "zip_code", hour ORDER BY trip_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) as moving_stddev_14d,
    AVG(trip_count) OVER (PARTITION BY "zip_code", hour ORDER BY trip_date ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING) as moving_avg_21d,
    STDDEV(trip_count) OVER (PARTITION BY "zip_code", hour ORDER BY trip_date ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING) as moving_stddev_21d
  FROM hourly_trips_complete
)
SELECT 
  hws."zip_code",
  hws.hour,
  hws.trip_count as total_trips,
  hws.trips_1h_ago,
  hws.trips_24h_ago,
  hws.trips_168h_ago,
  hws.trips_336h_ago,
  hws.moving_avg_14d,
  hws.moving_stddev_14d,
  hws.moving_avg_21d,
  hws.moving_stddev_21d
FROM hourly_with_stats hws
WHERE hws.trip_date = '2015-01-01'
ORDER BY hws.trip_count DESC
LIMIT 5