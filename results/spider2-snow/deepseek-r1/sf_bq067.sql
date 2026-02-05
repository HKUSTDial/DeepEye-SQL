WITH accidents_with_multiple_persons AS (
    SELECT 
        "state_number",
        "consecutive_number"
    FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."PERSON_2016"
    GROUP BY "state_number", "consecutive_number"
    HAVING COUNT(DISTINCT "person_number") > 1
),
fatal_counts AS (
    SELECT 
        "state_number",
        "consecutive_number",
        COUNT(*) AS "fatal_count"
    FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."PERSON_2016"
    WHERE "injury_severity" = 4
    GROUP BY "state_number", "consecutive_number"
),
accident_data AS (
    SELECT 
        "state_number",
        "consecutive_number",
        "number_of_drunk_drivers",
        "day_of_week",
        "hour_of_crash",
        CASE WHEN "work_zone" != 'None' THEN 1 ELSE 0 END AS "work_zone_indicator"
    FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."ACCIDENT_2016"
),
vehicle_body AS (
    SELECT 
        v1."state_number",
        v1."consecutive_number",
        v1."body_type"
    FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."VEHICLE_2016" v1
    INNER JOIN (
        SELECT 
            "state_number",
            "consecutive_number",
            MIN("vehicle_number") AS "min_vehicle_number"
        FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."VEHICLE_2016"
        GROUP BY "state_number", "consecutive_number"
    ) v2 ON v1."state_number" = v2."state_number" 
        AND v1."consecutive_number" = v2."consecutive_number" 
        AND v1."vehicle_number" = v2."min_vehicle_number"
),
speed_diffs AS (
    SELECT 
        "state_number",
        "consecutive_number",
        AVG(ABS("travel_speed" - "speed_limit")) AS "avg_speed_diff"
    FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."VEHICLE_2016"
    WHERE "travel_speed" <= 151 
        AND "travel_speed" NOT IN (997, 998, 999)
        AND "speed_limit" <= 80
        AND "speed_limit" NOT IN (98, 99)
    GROUP BY "state_number", "consecutive_number"
),
categorized_speed AS (
    SELECT 
        "state_number",
        "consecutive_number",
        "avg_speed_diff",
        CASE 
            WHEN "avg_speed_diff" >= 0 AND "avg_speed_diff" < 20 THEN 0
            WHEN "avg_speed_diff" >= 20 AND "avg_speed_diff" < 40 THEN 1
            WHEN "avg_speed_diff" >= 40 AND "avg_speed_diff" < 60 THEN 2
            WHEN "avg_speed_diff" >= 60 AND "avg_speed_diff" < 80 THEN 3
            WHEN "avg_speed_diff" >= 80 THEN 4
        END AS "speed_diff_level"
    FROM speed_diffs
)
SELECT 
    amp."state_number",
    amp."consecutive_number",
    ad."number_of_drunk_drivers",
    vb."body_type",
    ad."day_of_week",
    ad."hour_of_crash",
    ad."work_zone_indicator",
    cs."speed_diff_level",
    CASE WHEN COALESCE(fc."fatal_count", 0) > 1 THEN 1 ELSE 0 END AS "label"
FROM accidents_with_multiple_persons amp
INNER JOIN accident_data ad 
    ON amp."state_number" = ad."state_number" 
    AND amp."consecutive_number" = ad."consecutive_number"
LEFT JOIN fatal_counts fc 
    ON amp."state_number" = fc."state_number" 
    AND amp."consecutive_number" = fc."consecutive_number"
LEFT JOIN vehicle_body vb 
    ON amp."state_number" = vb."state_number" 
    AND amp."consecutive_number" = vb."consecutive_number"
LEFT JOIN categorized_speed cs 
    ON amp."state_number" = cs."state_number" 
    AND amp."consecutive_number" = cs."consecutive_number"