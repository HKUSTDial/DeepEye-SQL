WITH ages(age) AS (
    SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18
),
qualified_deaths AS (
    SELECT DISTINCT e."DeathRecordId"
    FROM "DEATH"."DEATH"."ENTITYAXISCONDITIONS" e
    INNER JOIN "DEATH"."DEATH"."ICD10CODE" i ON e."Icd10Code" = i."Code"
    WHERE (i."Description" ILIKE '%vehicle%' OR i."Description" ILIKE '%firearm%')
)
SELECT 
    a.age AS "Age",
    COUNT(DISTINCT CASE WHEN q."DeathRecordId" IS NOT NULL THEN d."Id" END) AS "TotalDeaths",
    COUNT(DISTINCT CASE WHEN q."DeathRecordId" IS NOT NULL AND r."Description" ILIKE '%black%' THEN d."Id" END) AS "BlackDeaths"
FROM ages a
LEFT JOIN "DEATH"."DEATH"."DEATHRECORDS" d ON d."Age" = a.age
LEFT JOIN qualified_deaths q ON d."Id" = q."DeathRecordId"
LEFT JOIN "DEATH"."DEATH"."RACE" r ON CAST(d."Race" AS VARCHAR) = r."Code"
GROUP BY a.age
ORDER BY a.age