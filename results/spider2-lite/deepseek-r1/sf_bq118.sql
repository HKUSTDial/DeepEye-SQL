WITH white_deaths AS (
    SELECT d."Id", d."Icd10Code", d."AgeRecode27"
    FROM "DEATH"."DEATH"."DEATHRECORDS" d
    INNER JOIN "DEATH"."DEATH"."RACE" r ON d."Race" = r."Code"
    WHERE r."Description" ILIKE '%white%'
),
discharge_codes AS (
    SELECT "Code"
    FROM "DEATH"."DEATH"."ICD10CODE"
    WHERE "Description" ILIKE '%discharge%'
      AND "Description" NOT ILIKE '%Urethral discharge%'
      AND "Description" NOT ILIKE '%Discharge of firework%'
      AND "Description" NOT ILIKE '%Legal intervention involving firearm discharge%'
),
vehicle_codes AS (
    SELECT "Code"
    FROM "DEATH"."DEATH"."ICD10CODE"
    WHERE "Description" ILIKE '%vehicle%'
),
death_flags AS (
    SELECT wd."Id", wd."AgeRecode27",
           CASE WHEN (wd."Icd10Code" IN (SELECT "Code" FROM discharge_codes)
                      OR EXISTS (SELECT 1 FROM "DEATH"."DEATH"."ENTITYAXISCONDITIONS" eac WHERE eac."DeathRecordId" = wd."Id" AND eac."Icd10Code" IN (SELECT "Code" FROM discharge_codes))
                      OR EXISTS (SELECT 1 FROM "DEATH"."DEATH"."RECORDAXISCONDITIONS" rac WHERE rac."DeathRecordId" = wd."Id" AND rac."Icd10Code" IN (SELECT "Code" FROM discharge_codes))
                     ) THEN 1 ELSE 0 END AS has_discharge,
           CASE WHEN (wd."Icd10Code" IN (SELECT "Code" FROM vehicle_codes)
                      OR EXISTS (SELECT 1 FROM "DEATH"."DEATH"."ENTITYAXISCONDITIONS" eac WHERE eac."DeathRecordId" = wd."Id" AND eac."Icd10Code" IN (SELECT "Code" FROM vehicle_codes))
                      OR EXISTS (SELECT 1 FROM "DEATH"."DEATH"."RECORDAXISCONDITIONS" rac WHERE rac."DeathRecordId" = wd."Id" AND rac."Icd10Code" IN (SELECT "Code" FROM vehicle_codes))
                     ) THEN 1 ELSE 0 END AS has_vehicle
    FROM white_deaths wd
),
counts_by_age AS (
    SELECT "AgeRecode27",
           SUM(has_discharge) AS discharge_count,
           SUM(has_vehicle) AS vehicle_count
    FROM death_flags
    GROUP BY "AgeRecode27"
)
SELECT AVG(discharge_count) - AVG(vehicle_count) AS difference
FROM counts_by_age