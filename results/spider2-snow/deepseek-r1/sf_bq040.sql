WITH filtered_trips AS (
    SELECT 
        "pickup_location_id",
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
        "passenger_count" > 0
        AND "trip_distance" >= 0
        AND "tip_amount" >= 0
        AND "tolls_amount" >= 0
        AND "mta_tax" >= 0
        AND "fare_amount" >= 0
        AND "total_amount" >= 0
        AND "dropoff_datetime" > "pickup_datetime"
        AND DATE(TO_TIMESTAMP("pickup_datetime" / 1000000)) BETWEEN DATE '2016-01-01' AND DATE '2016-01-07'
),
joined_trips AS (
    SELECT 
        f.*,
        z."borough",
        z."zone_name"
    FROM filtered_trips f
    INNER JOIN "NEW_YORK_PLUS"."NEW_YORK_TAXI_TRIPS"."TAXI_ZONE_GEOM" z 
        ON f."pickup_location_id" = z."zone_id"
    WHERE 
        z."borough" != 'Staten Island'
        AND z."zone_name" != 'EWR'
),
trips_with_tip_rate AS (
    SELECT 
        "borough",
        "tip_amount",
        "total_amount",
        CASE 
            WHEN "total_amount" = 0 THEN 0 
            ELSE ("tip_amount" / "total_amount") * 100 
        END AS tip_rate
    FROM joined_trips
),
trips_with_category AS (
    SELECT 
        "borough",
        tip_rate,
        CASE 
            WHEN tip_rate = 0 THEN '0% (no tip)'
            WHEN tip_rate <= 5 THEN 'up to 5%'
            WHEN tip_rate <= 10 THEN '5% to 10%'
            WHEN tip_rate <= 15 THEN '10% to 15%'
            WHEN tip_rate <= 20 THEN '15% to 20%'
            WHEN tip_rate <= 25 THEN '20% to 25%'
            ELSE 'more than 25%'
        END AS tip_category
    FROM trips_with_tip_rate
)
SELECT 
    "borough" AS pickup_borough,
    tip_category,
    COUNT(*) AS trip_count,
    COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY "borough") AS proportion
FROM trips_with_category
GROUP BY "borough", tip_category
ORDER BY 
    "borough",
    CASE tip_category
        WHEN '0% (no tip)' THEN 1
        WHEN 'up to 5%' THEN 2
        WHEN '5% to 10%' THEN 3
        WHEN '10% to 15%' THEN 4
        WHEN '15% to 20%' THEN 5
        WHEN '20% to 25%' THEN 6
        WHEN 'more than 25%' THEN 7
    END