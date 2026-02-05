WITH filtered_trips AS (
    SELECT 
        "trip_id",
        "duration_sec",
        "start_date",
        TO_TIMESTAMP("start_date", 6) AS "start_timestamp",
        "start_station_name",
        "end_station_name",
        "bike_number",
        "subscriber_type",
        "member_birth_year",
        "member_gender",
        "start_station_id"
    FROM 
        "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_TRIPS"
    WHERE 
        DATE(TO_TIMESTAMP("start_date", 6)) BETWEEN DATE '2017-07-01' AND DATE '2017-12-31'
        AND "start_station_name" IS NOT NULL
        AND "member_birth_year" IS NOT NULL
        AND TRY_CAST("member_birth_year" AS INTEGER) IS NOT NULL
        AND "member_gender" IS NOT NULL
        AND "member_gender" != 'NULL'
),
trips_with_age AS (
    SELECT 
        *,
        EXTRACT(YEAR FROM CURRENT_DATE()) - CAST("member_birth_year" AS INTEGER) AS "age",
        CONCAT("start_station_name", ' - ', "end_station_name") AS "route"
    FROM 
        filtered_trips
),
trips_with_region AS (
    SELECT 
        t."trip_id",
        t."duration_sec",
        t."start_timestamp" AS "start_date",
        t."start_station_name",
        t."route",
        t."bike_number",
        t."subscriber_type",
        t."member_birth_year",
        t."age",
        CASE 
            WHEN t."age" < 40 THEN 'Young (<40 Y.O)'
            WHEN t."age" BETWEEN 40 AND 60 THEN 'Adult (40-60 Y.O)'
            ELSE 'Senior Adult (>60 Y.O)'
        END AS "age_classification",
        t."member_gender",
        r."name" AS "region_name"
    FROM 
        trips_with_age t
    LEFT JOIN 
        "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_STATION_INFO" si 
        ON CAST(t."start_station_id" AS TEXT) = si."station_id"
    LEFT JOIN 
        "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_REGIONS" r 
        ON si."region_id" = r."region_id"
)
SELECT 
    "trip_id",
    "duration_sec",
    "start_date",
    "start_station_name",
    "route",
    "bike_number",
    "subscriber_type",
    "member_birth_year",
    "age",
    "age_classification",
    "member_gender",
    "region_name"
FROM 
    trips_with_region
ORDER BY 
    "duration_sec" DESC
LIMIT 5