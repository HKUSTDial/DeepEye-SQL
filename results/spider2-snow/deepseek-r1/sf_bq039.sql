WITH all_trips AS (
    SELECT 
        "pickup_location_id",
        "dropoff_location_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "tip_amount",
        "tolls_amount",
        "mta_tax",
        "fare_amount",
        "total_amount"
    FROM "NEW_YORK_PLUS"."NEW_YORK_TAXI_TRIPS"."TLC_YELLOW_TRIPS_2016"
    WHERE 
        TO_TIMESTAMP("pickup_datetime" / 1000000)::DATE BETWEEN '2016-07-01' AND '2016-07-07'
        AND TO_TIMESTAMP("dropoff_datetime" / 1000000)::DATE BETWEEN '2016-07-01' AND '2016-07-07'
        AND "passenger_count" > 5
        AND "trip_distance" >= 10
        AND "tip_amount" >= 0
        AND "tolls_amount" >= 0
        AND "mta_tax" >= 0
        AND "fare_amount" >= 0
        AND "total_amount" >= 0
        AND "dropoff_datetime" > "pickup_datetime"
    UNION ALL
    SELECT 
        "pickup_location_id",
        "dropoff_location_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "tip_amount",
        "tolls_amount",
        "mta_tax",
        "fare_amount",
        "total_amount"
    FROM "NEW_YORK_PLUS"."NEW_YORK_TAXI_TRIPS"."TLC_GREEN_TRIPS_2016"
    WHERE 
        TO_TIMESTAMP("pickup_datetime" / 1000000)::DATE BETWEEN '2016-07-01' AND '2016-07-07'
        AND TO_TIMESTAMP("dropoff_datetime" / 1000000)::DATE BETWEEN '2016-07-01' AND '2016-07-07'
        AND "passenger_count" > 5
        AND "trip_distance" >= 10
        AND "tip_amount" >= 0
        AND "tolls_amount" >= 0
        AND "mta_tax" >= 0
        AND "fare_amount" >= 0
        AND "total_amount" >= 0
        AND "dropoff_datetime" > "pickup_datetime"
)
SELECT 
    pz."zone_name" AS "pickup_zone",
    dz."zone_name" AS "dropoff_zone",
    (t."dropoff_datetime" - t."pickup_datetime") / 1000000 AS "duration_seconds",
    (t."trip_distance" * 3600) / ((t."dropoff_datetime" - t."pickup_datetime") / 1000000) AS "speed_mph",
    CASE 
        WHEN t."total_amount" > 0 THEN (t."tip_amount" / t."total_amount") * 100 
        ELSE NULL 
    END AS "tip_rate_pct"
FROM all_trips t
INNER JOIN "NEW_YORK_PLUS"."NEW_YORK_TAXI_TRIPS"."TAXI_ZONE_GEOM" pz
    ON t."pickup_location_id" = pz."zone_id"
INNER JOIN "NEW_YORK_PLUS"."NEW_YORK_TAXI_TRIPS"."TAXI_ZONE_GEOM" dz
    ON t."dropoff_location_id" = dz."zone_id"
ORDER BY t."total_amount" DESC
LIMIT 10