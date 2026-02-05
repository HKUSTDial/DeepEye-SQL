WITH hail_events AS (
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2014"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2015"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2016"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2017"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2018"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2019"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2020"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2021"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2022"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2023"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
  UNION ALL
  SELECT TO_GEOGRAPHY("event_point") AS event_geo
  FROM "NOAA_DATA_PLUS"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2024"
  WHERE LOWER("event_type") LIKE '%hail%' AND "event_point" IS NOT NULL
    AND TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -10, CURRENT_DATE())
)
SELECT "zip_code", COUNT(*) AS "event_count"
FROM hail_events
JOIN "NOAA_DATA_PLUS"."GEO_US_BOUNDARIES"."ZIP_CODES"
  ON ST_WITHIN(hail_events.event_geo, TO_GEOGRAPHY("NOAA_DATA_PLUS"."GEO_US_BOUNDARIES"."ZIP_CODES"."zip_code_geom"))
GROUP BY "zip_code"
ORDER BY "event_count" DESC
LIMIT 5