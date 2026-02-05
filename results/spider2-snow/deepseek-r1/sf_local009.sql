WITH "abakan_airports" AS (
  SELECT "airport_code"
  FROM "AIRLINES"."AIRLINES"."AIRPORTS_DATA"
  WHERE PARSE_JSON("city"):"en"::STRING = 'Abakan'
), "flights_with_abakan" AS (
  SELECT "flight_id", "departure_airport", "arrival_airport"
  FROM "AIRLINES"."AIRLINES"."FLIGHTS"
  WHERE "departure_airport" IN (SELECT "airport_code" FROM "abakan_airports")
     OR "arrival_airport" IN (SELECT "airport_code" FROM "abakan_airports")
), "flight_coords" AS (
  SELECT f."flight_id", dep."coordinates" AS "dep_coords", arr."coordinates" AS "arr_coords"
  FROM "flights_with_abakan" f
  JOIN "AIRLINES"."AIRLINES"."AIRPORTS_DATA" dep ON f."departure_airport" = dep."airport_code"
  JOIN "AIRLINES"."AIRLINES"."AIRPORTS_DATA" arr ON f."arrival_airport" = arr."airport_code"
), "parsed_coords" AS (
  SELECT "flight_id",
         TRY_CAST(SPLIT_PART(REGEXP_REPLACE("dep_coords", '[()]', ''), ',', 1) AS FLOAT) AS "dep_lon",
         TRY_CAST(SPLIT_PART(REGEXP_REPLACE("dep_coords", '[()]', ''), ',', 2) AS FLOAT) AS "dep_lat",
         TRY_CAST(SPLIT_PART(REGEXP_REPLACE("arr_coords", '[()]', ''), ',', 1) AS FLOAT) AS "arr_lon",
         TRY_CAST(SPLIT_PART(REGEXP_REPLACE("arr_coords", '[()]', ''), ',', 2) AS FLOAT) AS "arr_lat"
  FROM "flight_coords"
), "distances" AS (
  SELECT "flight_id",
         2 * 6371 * ASIN(SQRT(
           SIN(RADIANS("arr_lat" - "dep_lat") / 2) * SIN(RADIANS("arr_lat" - "dep_lat") / 2) +
           COS(RADIANS("dep_lat")) * COS(RADIANS("arr_lat")) *
           SIN(RADIANS("arr_lon" - "dep_lon") / 2) * SIN(RADIANS("arr_lon" - "dep_lon") / 2)
         )) AS "distance_km"
  FROM "parsed_coords"
)
SELECT MAX("distance_km") AS "longest_route_distance_km"
FROM "distances"