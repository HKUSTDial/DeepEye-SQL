WITH airports AS (
    SELECT 
        "airport_code",
        PARSE_JSON("city"):en::VARCHAR AS "city_name",
        CAST(SPLIT_PART(TRIM("coordinates", '()'), ',', 1) AS FLOAT) AS "lon",
        CAST(SPLIT_PART(TRIM("coordinates", '()'), ',', 2) AS FLOAT) AS "lat"
    FROM "AIRLINES"."AIRLINES"."AIRPORTS_DATA"
), flights_cities AS (
    SELECT 
        f."flight_id",
        dep."city_name" AS dep_city,
        arr."city_name" AS arr_city,
        dep."lon" AS dep_lon,
        dep."lat" AS dep_lat,
        arr."lon" AS arr_lon,
        arr."lat" AS arr_lat
    FROM "AIRLINES"."AIRLINES"."FLIGHTS" f
    JOIN airports dep ON f."departure_airport" = dep."airport_code"
    JOIN airports arr ON f."arrival_airport" = arr."airport_code"
    WHERE dep."city_name" != arr."city_name"
), flight_distances AS (
    SELECT 
        "flight_id",
        dep_city,
        arr_city,
        2 * 6371 * ASIN(SQRT(
            POWER(SIN(RADIANS(arr_lat - dep_lat)/2), 2) +
            COS(RADIANS(dep_lat)) * COS(RADIANS(arr_lat)) *
            POWER(SIN(RADIANS(arr_lon - dep_lon)/2), 2)
        )) AS distance_km
    FROM flights_cities
), city_pairs AS (
    SELECT 
        LEAST(dep_city, arr_city) AS city1,
        GREATEST(dep_city, arr_city) AS city2,
        distance_km
    FROM flight_distances
), avg_distance_per_pair AS (
    SELECT 
        city1,
        city2,
        AVG(distance_km) AS avg_distance_km
    FROM city_pairs
    GROUP BY city1, city2
), buckets AS (
    SELECT 
        city1,
        city2,
        avg_distance_km,
        CASE 
            WHEN avg_distance_km < 1000 THEN '0'
            WHEN avg_distance_km < 2000 THEN '1000'
            WHEN avg_distance_km < 3000 THEN '2000'
            WHEN avg_distance_km < 4000 THEN '3000'
            WHEN avg_distance_km < 5000 THEN '4000'
            WHEN avg_distance_km < 6000 THEN '5000'
            ELSE '6000+'
        END AS distance_range
    FROM avg_distance_per_pair
), counts_per_range AS (
    SELECT 
        distance_range,
        COUNT(*) AS pair_count
    FROM buckets
    GROUP BY distance_range
), all_ranges AS (
    SELECT '0' AS distance_range UNION ALL
    SELECT '1000' UNION ALL
    SELECT '2000' UNION ALL
    SELECT '3000' UNION ALL
    SELECT '4000' UNION ALL
    SELECT '5000' UNION ALL
    SELECT '6000+'
), counts_with_ranges AS (
    SELECT 
        ar.distance_range,
        COALESCE(cpr.pair_count, 0) AS pair_count
    FROM all_ranges ar
    LEFT JOIN counts_per_range cpr ON ar.distance_range = cpr.distance_range
)
SELECT MIN(pair_count) AS min_pair_count FROM counts_with_ranges