WITH age_series AS (
    SELECT 12 AS "Age" UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18
), deaths AS (
    SELECT d."Id" AS "DeathRecordId", d."Age", 
           CASE WHEN "R"."Description" ILIKE '%black%' THEN 1 ELSE 0 END AS "IsBlack"
    FROM "DEATH"."DEATH"."DEATHRECORDS" d
    JOIN "DEATH"."DEATH"."RACE" "R" ON d."Race" = "R"."Code"
    WHERE d."Age" BETWEEN 12 AND 18
), vehicle_deaths AS (
    SELECT d."Age", COUNT(*) AS "TotalDeaths", SUM(d."IsBlack") AS "BlackDeaths"
    FROM deaths d
    WHERE EXISTS (
        SELECT 1 FROM "DEATH"."DEATH"."ENTITYAXISCONDITIONS" e
        JOIN "DEATH"."DEATH"."ICD10CODE" i ON e."Icd10Code" = CAST(i."Code" AS VARCHAR)
        WHERE e."DeathRecordId" = d."DeathRecordId" AND i."Description" ILIKE '%vehicle%'
    )
    GROUP BY d."Age"
), firearm_deaths AS (
    SELECT d."Age", COUNT(*) AS "TotalDeaths", SUM(d."IsBlack") AS "BlackDeaths"
    FROM deaths d
    WHERE EXISTS (
        SELECT 1 FROM "DEATH"."DEATH"."ENTITYAXISCONDITIONS" e
        JOIN "DEATH"."DEATH"."ICD10CODE" i ON e."Icd10Code" = CAST(i."Code" AS VARCHAR)
        WHERE e."DeathRecordId" = d."DeathRecordId" AND i."Description" ILIKE '%firearm%'
    )
    GROUP BY d."Age"
), combined AS (
    SELECT a."Age", 'vehicle' AS "Cause", COALESCE(v."TotalDeaths", 0) AS "TotalDeaths", COALESCE(v."BlackDeaths", 0) AS "BlackDeaths"
    FROM age_series a
    LEFT JOIN vehicle_deaths v ON a."Age" = v."Age"
    UNION ALL
    SELECT a."Age", 'firearm' AS "Cause", COALESCE(f."TotalDeaths", 0) AS "TotalDeaths", COALESCE(f."BlackDeaths", 0) AS "BlackDeaths"
    FROM age_series a
    LEFT JOIN firearm_deaths f ON a."Age" = f."Age"
)
SELECT "Age", "Cause", "TotalDeaths", "BlackDeaths"
FROM combined
ORDER BY "Age", "Cause"