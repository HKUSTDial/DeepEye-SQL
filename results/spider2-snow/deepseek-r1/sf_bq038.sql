WITH round_trips AS (
    SELECT 
        CAST("start_station_id" AS TEXT) AS "station_id",
        "bikeid",
        "starttime",
        "stoptime"
    FROM "NEW_YORK"."NEW_YORK"."CITIBIKE_TRIPS"
    WHERE "start_station_id" = "end_station_id"
      AND "start_station_id" IS NOT NULL
),
companion_counts AS (
    SELECT
        rt1."station_id",
        rt1."bikeid",
        rt1."starttime",
        rt1."stoptime",
        COUNT(rt2."bikeid") AS "companion_count"
    FROM round_trips rt1
    LEFT JOIN round_trips rt2 
        ON rt1."station_id" = rt2."station_id"
        AND rt1."bikeid" != rt2."bikeid"
        AND ABS(rt1."starttime" - rt2."starttime") <= 120000000
        AND ABS(rt1."stoptime" - rt2."stoptime") <= 120000000
    GROUP BY rt1."station_id", rt1."bikeid", rt1."starttime", rt1."stoptime"
),
group_trips_per_station AS (
    SELECT
        "station_id",
        COUNT(*) AS "group_trips"
    FROM companion_counts
    WHERE "companion_count" >= 1
    GROUP BY "station_id"
),
total_trips_per_station AS (
    SELECT
        CAST("end_station_id" AS TEXT) AS "station_id",
        COUNT(*) AS "total_trips"
    FROM "NEW_YORK"."NEW_YORK"."CITIBIKE_TRIPS"
    WHERE "end_station_id" IS NOT NULL
    GROUP BY "end_station_id"
),
station_proportions AS (
    SELECT
        COALESCE(gt."station_id", tt."station_id") AS "station_id",
        COALESCE(gt."group_trips", 0) AS "group_trips",
        tt."total_trips",
        COALESCE(gt."group_trips", 0) / tt."total_trips" AS "proportion"
    FROM total_trips_per_station tt
    LEFT JOIN group_trips_per_station gt ON tt."station_id" = gt."station_id"
)
SELECT 
    sp."station_id",
    cs."name" AS "station_name",
    sp."group_trips",
    sp."total_trips",
    sp."proportion"
FROM station_proportions sp
LEFT JOIN "NEW_YORK"."NEW_YORK"."CITIBIKE_STATIONS" cs 
    ON sp."station_id" = cs."station_id"
WHERE sp."total_trips" > 0
ORDER BY sp."proportion" DESC
LIMIT 10