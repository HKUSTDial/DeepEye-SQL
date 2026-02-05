WITH filtered_hurricanes AS (
    SELECT 
        "sid",
        "iso_time",
        "latitude",
        "longitude"
    FROM "NOAA_DATA"."NOAA_HURRICANES"."HURRICANES"
    WHERE "basin" = 'NA' AND "season" = '2020'
),
ordered_points AS (
    SELECT 
        "sid",
        "iso_time",
        "latitude",
        "longitude",
        LAG("longitude") OVER (PARTITION BY "sid" ORDER BY "iso_time") AS prev_lon,
        LAG("latitude") OVER (PARTITION BY "sid" ORDER BY "iso_time") AS prev_lat
    FROM filtered_hurricanes
),
distance_calculations AS (
    SELECT 
        "sid",
        "iso_time",
        "latitude",
        "longitude",
        CASE 
            WHEN prev_lon IS NOT NULL AND prev_lat IS NOT NULL 
            THEN ST_DISTANCE(
                ST_MAKEPOINT("longitude", "latitude"),
                ST_MAKEPOINT(prev_lon, prev_lat)
            )
            ELSE 0 
        END AS segment_distance
    FROM ordered_points
),
hurricane_totals AS (
    SELECT 
        "sid",
        SUM(segment_distance) AS total_distance
    FROM distance_calculations
    GROUP BY "sid"
),
ranked_hurricanes AS (
    SELECT 
        "sid",
        total_distance,
        ROW_NUMBER() OVER (ORDER BY total_distance DESC) AS distance_rank
    FROM hurricane_totals
)
SELECT 
    dc."latitude"
FROM distance_calculations dc
INNER JOIN ranked_hurricanes rh ON dc."sid" = rh."sid"
WHERE rh.distance_rank = 2
AND dc."iso_time" = (
    SELECT MAX("iso_time")
    FROM distance_calculations dc2
    WHERE dc2."sid" = dc."sid"
)