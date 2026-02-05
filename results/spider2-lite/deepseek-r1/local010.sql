WITH flight_coords AS (
    SELECT 
        json_extract(dep.`city`, '$.en') AS dep_city_en,
        json_extract(arr.`city`, '$.en') AS arr_city_en,
        CAST(SUBSTR(dep.`coordinates`, 2, INSTR(dep.`coordinates`, ',') - 2) AS REAL) AS dep_lon,
        CAST(SUBSTR(dep.`coordinates`, INSTR(dep.`coordinates`, ',') + 1, LENGTH(dep.`coordinates`) - INSTR(dep.`coordinates`, ',') - 1) AS REAL) AS dep_lat,
        CAST(SUBSTR(arr.`coordinates`, 2, INSTR(arr.`coordinates`, ',') - 2) AS REAL) AS arr_lon,
        CAST(SUBSTR(arr.`coordinates`, INSTR(arr.`coordinates`, ',') + 1, LENGTH(arr.`coordinates`) - INSTR(arr.`coordinates`, ',') - 1) AS REAL) AS arr_lat
    FROM `flights` f
    JOIN `airports_data` dep ON f.`departure_airport` = dep.`airport_code`
    JOIN `airports_data` arr ON f.`arrival_airport` = arr.`airport_code`
),
flight_distances AS (
    SELECT 
        dep_city_en,
        arr_city_en,
        2 * 6371 * ASIN(SQRT(
            SIN((arr_lat * 3.141592653589793 / 180.0 - dep_lat * 3.141592653589793 / 180.0) / 2) * 
            SIN((arr_lat * 3.141592653589793 / 180.0 - dep_lat * 3.141592653589793 / 180.0) / 2) +
            COS(dep_lat * 3.141592653589793 / 180.0) * 
            COS(arr_lat * 3.141592653589793 / 180.0) *
            SIN((arr_lon * 3.141592653589793 / 180.0 - dep_lon * 3.141592653589793 / 180.0) / 2) * 
            SIN((arr_lon * 3.141592653589793 / 180.0 - dep_lon * 3.141592653589793 / 180.0) / 2)
        )) AS distance
    FROM flight_coords
),
city_pairs AS (
    SELECT 
        MIN(dep_city_en, arr_city_en) AS city1,
        MAX(dep_city_en, arr_city_en) AS city2,
        AVG(distance) AS avg_distance
    FROM flight_distances
    GROUP BY MIN(dep_city_en, arr_city_en), MAX(dep_city_en, arr_city_en)
),
buckets AS (
    SELECT 
        city1,
        city2,
        avg_distance,
        CASE 
            WHEN avg_distance < 1000 THEN '0'
            WHEN avg_distance < 2000 THEN '1000'
            WHEN avg_distance < 3000 THEN '2000'
            WHEN avg_distance < 4000 THEN '3000'
            WHEN avg_distance < 5000 THEN '4000'
            WHEN avg_distance < 6000 THEN '5000'
            ELSE '6000+'
        END AS distance_range
    FROM city_pairs
),
range_counts AS (
    SELECT 
        distance_range,
        COUNT(*) AS pair_count
    FROM buckets
    GROUP BY distance_range
)
SELECT MIN(pair_count) AS fewest_pairs FROM range_counts;