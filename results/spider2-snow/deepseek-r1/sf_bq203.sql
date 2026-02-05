WITH "total_per_borough" AS (
    SELECT "borough_name", COUNT(DISTINCT "station_name") AS "total_stations"
    FROM "NEW_YORK_PLUS"."NEW_YORK_SUBWAY"."STATIONS"
    GROUP BY "borough_name"
),
"with_entry_ada_per_borough" AS (
    SELECT s."borough_name", COUNT(DISTINCT s."station_name") AS "stations_with_entry_ada"
    FROM "NEW_YORK_PLUS"."NEW_YORK_SUBWAY"."STATIONS" s
    JOIN "NEW_YORK_PLUS"."NEW_YORK_SUBWAY"."STATION_ENTRANCES" e ON s."station_name" = e."station_name"
    WHERE e."entry" = TRUE AND e."ada_compliant" = TRUE
    GROUP BY s."borough_name"
)
SELECT 
    t."borough_name",
    t."total_stations",
    COALESCE(w."stations_with_entry_ada", 0) AS "stations_with_entry_ada",
    CASE 
        WHEN t."total_stations" > 0 
        THEN (COALESCE(w."stations_with_entry_ada", 0) * 100.0 / t."total_stations") 
        ELSE 0 
    END AS "percentage"
FROM "total_per_borough" t
LEFT JOIN "with_entry_ada_per_borough" w ON t."borough_name" = w."borough_name"
ORDER BY "percentage" DESC