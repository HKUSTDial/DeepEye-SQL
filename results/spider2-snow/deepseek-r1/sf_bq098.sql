WITH filtered_trips AS (
    SELECT 
        t."pickup_location_id",
        t."tip_amount",
        t."total_amount"
    FROM "NEW_YORK_PLUS"."NEW_YORK_TAXI_TRIPS"."TLC_YELLOW_TRIPS_2016" AS t
    WHERE t."pickup_datetime" >= 1451606400000000
        AND t."pickup_datetime" < 1452211200000000
        AND t."dropoff_datetime" >= 1451606400000000
        AND t."dropoff_datetime" < 1452211200000000
        AND t."dropoff_datetime" > t."pickup_datetime"
        AND t."passenger_count" > 0
        AND t."trip_distance" >= 0
        AND t."tip_amount" >= 0
        AND t."tolls_amount" >= 0
        AND t."mta_tax" >= 0
        AND t."fare_amount" >= 0
        AND t."total_amount" >= 0
),
trips_with_borough AS (
    SELECT 
        f.*,
        COALESCE(z."borough", 'Unknown') AS borough
    FROM filtered_trips f
    LEFT JOIN "NEW_YORK_PLUS"."NEW_YORK_TAXI_TRIPS"."TAXI_ZONE_GEOM" z
        ON f."pickup_location_id" = z."zone_id"
),
trips_with_tip_rate AS (
    SELECT 
        *,
        CASE 
            WHEN "total_amount" = 0 THEN 0 
            ELSE ("tip_amount" * 100) / "total_amount" 
        END AS tip_rate
    FROM trips_with_borough
),
trips_with_tip_flag AS (
    SELECT 
        *,
        CASE 
            WHEN tip_rate = 0 THEN 1 
            ELSE 0 
        END AS no_tip_flag
    FROM trips_with_tip_rate
)
SELECT 
    borough,
    COUNT(*) AS total_trips,
    SUM(no_tip_flag) AS no_tip_trips,
    (no_tip_trips * 100.0) / COUNT(*) AS percentage_no_tip
FROM trips_with_tip_flag
GROUP BY borough
ORDER BY borough