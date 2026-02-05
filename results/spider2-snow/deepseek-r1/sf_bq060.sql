SELECT "bdgr"."country_name", "bdgr"."net_migration"
FROM "CENSUS_BUREAU_INTERNATIONAL"."CENSUS_BUREAU_INTERNATIONAL"."BIRTH_DEATH_GROWTH_RATES" AS "bdgr"
INNER JOIN "CENSUS_BUREAU_INTERNATIONAL"."CENSUS_BUREAU_INTERNATIONAL"."COUNTRY_NAMES_AREA" AS "cna"
ON "bdgr"."country_code" = "cna"."country_code"
WHERE "bdgr"."year" = 2017 AND "cna"."country_area" > 500
ORDER BY "bdgr"."net_migration" DESC
LIMIT 3