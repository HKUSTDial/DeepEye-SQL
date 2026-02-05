WITH ordered_geolocation AS (
    SELECT 
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,
        geolocation_zip_code_prefix,
        LAG(geolocation_lat) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_lat,
        LAG(geolocation_lng) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_lng,
        LAG(geolocation_city) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_city,
        LAG(geolocation_state) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_state,
        LAG(geolocation_zip_code_prefix) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_zip
    FROM `olist_geolocation`
),
distances AS (
    SELECT 
        prev_state,
        prev_city,
        prev_zip,
        prev_lat,
        prev_lng,
        geolocation_state AS curr_state,
        geolocation_city AS curr_city,
        geolocation_zip_code_prefix AS curr_zip,
        geolocation_lat AS curr_lat,
        geolocation_lng AS curr_lng,
        6371 * ACOS(
            COS(prev_lat * 3.141592653589793 / 180) * 
            COS(geolocation_lat * 3.141592653589793 / 180) * 
            COS(geolocation_lng * 3.141592653589793 / 180 - prev_lng * 3.141592653589793 / 180) + 
            SIN(prev_lat * 3.141592653589793 / 180) * 
            SIN(geolocation_lat * 3.141592653589793 / 180)
        ) AS distance
    FROM ordered_geolocation
    WHERE prev_lat IS NOT NULL AND prev_lng IS NOT NULL
)
SELECT 
    prev_state,
    prev_city,
    prev_zip,
    prev_lat,
    prev_lng,
    curr_state,
    curr_city,
    curr_zip,
    curr_lat,
    curr_lng,
    distance
FROM distances
WHERE distance = (SELECT MAX(distance) FROM distances)