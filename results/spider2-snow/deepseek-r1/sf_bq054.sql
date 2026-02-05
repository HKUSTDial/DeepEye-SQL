WITH all_latin AS (
    SELECT DISTINCT UPPER(TRIM("spc_latin")) AS "latin"
    FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_1995"
    WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "spc_latin" != 'PLANTING SITE'
    UNION
    SELECT DISTINCT UPPER(TRIM("spc_latin")) AS "latin"
    FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_2005"
    WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "status" IN ('Excellent', 'Good', 'Poor', 'Dead')
    UNION
    SELECT DISTINCT UPPER(TRIM("spc_latin")) AS "latin"
    FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_2015"
    WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "status" IN ('Alive', 'Dead')
),
common_mapping AS (
    SELECT "latin", "common", ROW_NUMBER() OVER (PARTITION BY "latin" ORDER BY "year" DESC) AS "rn"
    FROM (
        SELECT UPPER(TRIM("spc_latin")) AS "latin", TRIM("spc_common") AS "common", 2015 AS "year"
        FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_2015"
        WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "status" IN ('Alive', 'Dead')
        UNION ALL
        SELECT UPPER(TRIM("spc_latin")) AS "latin", TRIM("spc_common") AS "common", 2005 AS "year"
        FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_2005"
        WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "status" IN ('Excellent', 'Good', 'Poor', 'Dead')
        UNION ALL
        SELECT UPPER(TRIM("spc_latin")) AS "latin", TRIM("spc_common") AS "common", 1995 AS "year"
        FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_1995"
        WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "spc_latin" != 'PLANTING SITE' AND "spc_common" != 'PLANTING SITE'
    ) t
    WHERE "latin" IN (SELECT "latin" FROM all_latin)
),
common_name AS (
    SELECT "latin", "common"
    FROM common_mapping
    WHERE "rn" = 1
),
year_1995_agg AS (
    SELECT UPPER(TRIM("spc_latin")) AS "latin", COUNT(*) AS "total_1995", COUNT(*) AS "alive_1995", 0 AS "dead_1995"
    FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_1995"
    WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "spc_latin" != 'PLANTING SITE' AND "spc_common" != 'PLANTING SITE'
    GROUP BY "latin"
),
year_2005_agg AS (
    SELECT UPPER(TRIM("spc_latin")) AS "latin", COUNT(*) AS "total_2005", SUM(CASE WHEN "status" IN ('Excellent', 'Good', 'Poor') THEN 1 ELSE 0 END) AS "alive_2005", SUM(CASE WHEN "status" = 'Dead' THEN 1 ELSE 0 END) AS "dead_2005"
    FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_2005"
    WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "status" IN ('Excellent', 'Good', 'Poor', 'Dead')
    GROUP BY "latin"
),
year_2015_agg AS (
    SELECT UPPER(TRIM("spc_latin")) AS "latin", COUNT(*) AS "total_2015", SUM(CASE WHEN "status" = 'Alive' THEN 1 ELSE 0 END) AS "alive_2015", SUM(CASE WHEN "status" = 'Dead' THEN 1 ELSE 0 END) AS "dead_2015"
    FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_2015"
    WHERE "spc_latin" IS NOT NULL AND TRIM("spc_latin") != '' AND "status" IN ('Alive', 'Dead')
    GROUP BY "latin"
)
SELECT a."latin", cn."common", COALESCE(y1995."total_1995", 0) AS "total_1995", COALESCE(y1995."alive_1995", 0) AS "alive_1995", COALESCE(y1995."dead_1995", 0) AS "dead_1995", COALESCE(y2005."total_2005", 0) AS "total_2005", COALESCE(y2005."alive_2005", 0) AS "alive_2005", COALESCE(y2005."dead_2005", 0) AS "dead_2005", COALESCE(y2015."total_2015", 0) AS "total_2015", COALESCE(y2015."alive_2015", 0) AS "alive_2015", COALESCE(y2015."dead_2015", 0) AS "dead_2015", (COALESCE(y2015."total_2015", 0) - COALESCE(y1995."total_1995", 0)) AS "growth_total", (COALESCE(y2015."alive_2015", 0) - COALESCE(y1995."alive_1995", 0)) AS "growth_alive", (COALESCE(y2015."dead_2015", 0) - COALESCE(y1995."dead_1995", 0)) AS "growth_dead"
FROM all_latin a
LEFT JOIN common_name cn ON a."latin" = cn."latin"
LEFT JOIN year_1995_agg y1995 ON a."latin" = y1995."latin"
LEFT JOIN year_2005_agg y2005 ON a."latin" = y2005."latin"
LEFT JOIN year_2015_agg y2015 ON a."latin" = y2015."latin"
ORDER BY "growth_total" DESC
LIMIT 10