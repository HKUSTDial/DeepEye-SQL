WITH berkeley_stations AS (
    SELECT CAST(si."station_id" AS NUMBER) AS station_id_num
    FROM "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_STATION_INFO" si
    JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_REGIONS" r
        ON si."region_id" = r."region_id"
    WHERE r."name" = 'Berkeley'
),
trip_distances AS (
    SELECT 
        t."trip_id",
        t."duration_sec",
        t."start_station_id",
        t."end_station_id",
        2 * 6371000 * ASIN(SQRT(
            POW(SIN(RADIANS(t."end_station_latitude" - t."start_station_latitude") / 2), 2) +
            COS(RADIANS(t."start_station_latitude")) * COS(RADIANS(t."end_station_latitude")) *
            POW(SIN(RADIANS(t."end_station_longitude" - t."start_station_longitude") / 2), 2)
        )) AS "distance_m"
    FROM "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_TRIPS" t
),
qualified_trips AS (
    SELECT 
        td."trip_id",
        td."duration_sec",
        td."distance_m",
        td."distance_m" / td."duration_sec" AS speed_mps
    FROM trip_distances td
    WHERE td."distance_m" > 1000
      AND td."duration_sec" > 0
      AND (td."start_station_id" IN (SELECT station_id_num FROM berkeley_stations)
           OR td."end_station_id" IN (SELECT station_id_num FROM berkeley_stations))
)
SELECT 
    ROUND(MAX(speed_mps), 1) AS highest_avg_speed
FROM qualified_trips