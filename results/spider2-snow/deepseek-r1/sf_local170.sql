WITH intervals AS (
    SELECT 0 AS years UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT 6 UNION ALL SELECT 8 UNION ALL SELECT 10
),
first_starts AS (
    SELECT 
        "id_bioguide",
        "state",
        MIN(TO_DATE("term_start")) AS first_start
    FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."LEGISLATORS_TERMS"
    GROUP BY "id_bioguide", "state"
),
legislators_with_intervals AS (
    SELECT 
        fs."id_bioguide",
        fs."state",
        l."gender",
        fs.first_start,
        i.years,
        DATE_FROM_PARTS(YEAR(fs.first_start) + i.years, 12, 31) AS retention_date
    FROM first_starts fs
    JOIN intervals i
    JOIN "CITY_LEGISLATION"."CITY_LEGISLATION"."LEGISLATORS" l ON fs."id_bioguide" = l."id_bioguide"
),
retention_check AS (
    SELECT 
        lwi."state",
        lwi."gender",
        lwi.years,
        lwi."id_bioguide",
        CASE WHEN EXISTS (
            SELECT 1 
            FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."LEGISLATORS_TERMS" lt 
            WHERE lt."id_bioguide" = lwi."id_bioguide"
                AND lt."state" = lwi."state"
                AND TO_DATE(lt."term_start") <= lwi.retention_date
                AND TO_DATE(lt."term_end") >= lwi.retention_date
        ) THEN 1 ELSE 0 END AS retained
    FROM legislators_with_intervals lwi
),
retention_aggregated AS (
    SELECT 
        "state",
        "gender",
        years,
        COUNT(DISTINCT "id_bioguide") AS total_legislators,
        SUM(retained) AS retained_count
    FROM retention_check
    GROUP BY "state", "gender", years
),
state_gender_compliance AS (
    SELECT 
        "state",
        "gender",
        MIN(retained_count) AS min_retained
    FROM retention_aggregated
    WHERE years IN (0,2,4,6,8,10)
    GROUP BY "state", "gender"
    HAVING COUNT(*) = 6 AND min_retained > 0
)
SELECT "state"
FROM state_gender_compliance
WHERE "gender" IN ('M', 'F')
GROUP BY "state"
HAVING COUNT(DISTINCT "gender") = 2
ORDER BY "state"