WITH hail_events AS (
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2014"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2015"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2016"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2017"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2018"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2019"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2020"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2021"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2022"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  
  UNION ALL
  
  SELECT ST_MAKEPOINT("event_longitude", "event_latitude") as event_point
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2023"
  WHERE "event_type" = 'hail'
    AND "event_latitude" IS NOT NULL
    AND "event_longitude" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
)
SELECT 
  z."zip_code",
  COUNT(*) as hail_event_count
FROM hail_events h
JOIN "NOAA_DATA_PLUS"."GEO_US_BOUNDARIES"."ZIP_CODES" z
  ON ST_WITHIN(h.event_point, TO_GEOGRAPHY(z."zip_code_geom"))
GROUP BY z."zip_code"
ORDER BY hail_event_count DESC
LIMIT 5