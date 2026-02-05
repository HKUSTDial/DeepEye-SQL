WITH filtered_hurricanes AS (
    SELECT 
        "sid",
        "name",
        "iso_time",
        "latitude",
        "longitude",
        "usa_wind"
    FROM "NOAA_DATA"."NOAA_HURRICANES"."HURRICANES"
    WHERE "basin" = 'NA'
        AND "season" = '2020'
        AND "track_type" = 'main'
),
ordered_points AS (
    SELECT 
        *,
        LAG("longitude") OVER (PARTITION BY "sid" ORDER BY "iso_time") AS prev_longitude,
        LAG("latitude") OVER (PARTITION BY "sid" ORDER BY "iso_time") AS prev_latitude
    FROM filtered_hurricanes
),
distances AS (
    SELECT 
        *,
        CASE 
            WHEN prev_longitude IS NULL OR prev_latitude IS NULL THEN 0
            ELSE ST_DISTANCE(
                ST_MAKEPOINT("longitude", "latitude"),
                ST_MAKEPOINT(prev_longitude, prev_latitude)
            ) / 1000
        END AS distance_km
    FROM ordered_points
),
hurricane_totals AS (
    SELECT 
        "sid",
        SUM(distance_km) AS total_travel_distance_km
    FROM distances
    GROUP BY "sid"
),
ranked_hurricanes AS (
    SELECT 
        "sid",
        total_travel_distance_km,
        RANK() OVER (ORDER BY total_travel_distance_km DESC) AS rank
    FROM hurricane_totals
),
third_hurricane AS (
    SELECT "sid", total_travel_distance_km
    FROM ranked_hurricanes
    WHERE rank = 3
)
SELECT 
    d."sid",
    d."name",
    d."iso_time",
    d."latitude",
    d."longitude",
    d."usa_wind",
    SUM(d.distance_km) OVER (PARTITION BY d."sid" ORDER BY d."iso_time") AS cumulative_distance_km
FROM distances d
INNER JOIN third_hurricane th ON d."sid" = th."sid"
ORDER BY d."iso_time"