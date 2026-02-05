WITH ordered_with_prev AS (
  SELECT 
    "geolocation_state",
    "geolocation_city",
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    LAG("geolocation_state") OVER (ORDER BY "geolocation_state", "geolocation_city", "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng") AS "prev_state",
    LAG("geolocation_city") OVER (ORDER BY "geolocation_state", "geolocation_city", "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng") AS "prev_city",
    LAG("geolocation_zip_code_prefix") OVER (ORDER BY "geolocation_state", "geolocation_city", "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng") AS "prev_zip",
    LAG("geolocation_lat") OVER (ORDER BY "geolocation_state", "geolocation_city", "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng") AS "prev_lat",
    LAG("geolocation_lng") OVER (ORDER BY "geolocation_state", "geolocation_city", "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng") AS "prev_lng"
  FROM "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_GEOLOCATION"
),
with_distance AS (
  SELECT 
    *,
    6371 * ACOS(
      LEAST(GREATEST(
        COS(RADIANS("prev_lat")) * COS(RADIANS("geolocation_lat")) * COS(RADIANS("geolocation_lng") - RADIANS("prev_lng")) +
        SIN(RADIANS("prev_lat")) * SIN(RADIANS("geolocation_lat")),
        -1
      ), 1)
    ) AS "distance_km"
  FROM ordered_with_prev
  WHERE "prev_lat" IS NOT NULL
)
SELECT 
  "prev_state" AS "city1_state",
  "prev_city" AS "city1_city",
  "prev_zip" AS "city1_zip_code_prefix",
  "prev_lat" AS "city1_lat",
  "prev_lng" AS "city1_lng",
  "geolocation_state" AS "city2_state",
  "geolocation_city" AS "city2_city",
  "geolocation_zip_code_prefix" AS "city2_zip_code_prefix",
  "geolocation_lat" AS "city2_lat",
  "geolocation_lng" AS "city2_lng",
  "distance_km"
FROM with_distance
ORDER BY "distance_km" DESC
LIMIT 1