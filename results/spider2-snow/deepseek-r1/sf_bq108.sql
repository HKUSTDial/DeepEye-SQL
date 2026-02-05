WITH eligible_accidents AS (
    SELECT "consecutive_number"
    FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."ACCIDENT_2015"
    WHERE "month_of_crash" BETWEEN 1 AND 8
      AND "year_of_crash" = 2015
),
person_counts AS (
    SELECT p."consecutive_number", COUNT(DISTINCT p."person_number") AS num_persons
    FROM eligible_accidents ea
    INNER JOIN "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."PERSON_2015" p
        ON ea."consecutive_number" = p."consecutive_number"
    GROUP BY p."consecutive_number"
),
accidents_multiple_persons AS (
    SELECT "consecutive_number"
    FROM person_counts
    WHERE num_persons > 1
),
severe_counts AS (
    SELECT p."consecutive_number", COUNT(*) AS num_severe
    FROM accidents_multiple_persons amp
    INNER JOIN "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."PERSON_2015" p
        ON amp."consecutive_number" = p."consecutive_number"
    WHERE p."injury_severity" = 4
    GROUP BY p."consecutive_number"
),
accidents_multiple_severe AS (
    SELECT "consecutive_number"
    FROM severe_counts
    WHERE num_severe > 1
),
counts AS (
    SELECT 
        (SELECT COUNT(*) FROM accidents_multiple_persons) AS total_multiple_person,
        (SELECT COUNT(*) FROM accidents_multiple_severe) AS total_multiple_severe
)
SELECT 
    (total_multiple_severe * 100.0) / NULLIF(total_multiple_person, 0) AS percentage
FROM counts