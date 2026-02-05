WITH "family_earliest" AS (
    SELECT "family_id", MIN("publication_date") AS "earliest_date"
    FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS"
    GROUP BY "family_id"
    HAVING FLOOR(MIN("publication_date") / 10000) = 2015
       AND FLOOR((MIN("publication_date") % 10000) / 100) = 1
),
"family_pubs" AS (
    SELECT 
        p."family_id",
        LISTAGG(DISTINCT p."publication_number", ', ') WITHIN GROUP (ORDER BY p."publication_number") AS "publication_numbers",
        LISTAGG(DISTINCT p."country_code", ', ') WITHIN GROUP (ORDER BY p."country_code") AS "country_codes"
    FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
    INNER JOIN "family_earliest" fe ON p."family_id" = fe."family_id"
    GROUP BY p."family_id"
),
"family_cpc" AS (
    SELECT 
        p."family_id",
        LISTAGG(DISTINCT f_cpc.value::STRING, ', ') WITHIN GROUP (ORDER BY f_cpc.value::STRING) AS "cpc_codes"
    FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
    INNER JOIN "family_earliest" fe ON p."family_id" = fe."family_id"
    LEFT JOIN LATERAL FLATTEN(INPUT => p."cpc") f_cpc
    GROUP BY p."family_id"
),
"family_ipc" AS (
    SELECT 
        p."family_id",
        LISTAGG(DISTINCT f_ipc.value::STRING, ', ') WITHIN GROUP (ORDER BY f_ipc.value::STRING) AS "ipc_codes"
    FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
    INNER JOIN "family_earliest" fe ON p."family_id" = fe."family_id"
    LEFT JOIN LATERAL FLATTEN(INPUT => p."ipc") f_ipc
    GROUP BY p."family_id"
),
"citing_info" AS (
    SELECT 
        p."family_id" AS "cited_family_id",
        LISTAGG(DISTINCT p2."family_id", ', ') WITHIN GROUP (ORDER BY p2."family_id") AS "citing_families"
    FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
    INNER JOIN "family_earliest" fe ON p."family_id" = fe."family_id"
    INNER JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" a ON p."publication_number" = a."publication_number"
    LEFT JOIN LATERAL FLATTEN(INPUT => a."cited_by") f_cited
    LEFT JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p2 ON f_cited.value::STRING = p2."publication_number"
    WHERE p2."family_id" IS NOT NULL
    GROUP BY p."family_id"
),
"cited_info" AS (
    SELECT 
        p."family_id" AS "citing_family_id",
        LISTAGG(DISTINCT p2."family_id", ', ') WITHIN GROUP (ORDER BY p2."family_id") AS "cited_families"
    FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
    INNER JOIN "family_earliest" fe ON p."family_id" = fe."family_id"
    LEFT JOIN LATERAL FLATTEN(INPUT => p."citation") f_cite
    LEFT JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p2 ON f_cite.value::STRING = p2."publication_number"
    WHERE p2."family_id" IS NOT NULL
    GROUP BY p."family_id"
)
SELECT 
    fe."family_id",
    fe."earliest_date",
    COALESCE(fp."publication_numbers", '') AS "publication_numbers",
    COALESCE(fp."country_codes", '') AS "country_codes",
    COALESCE(fcpc."cpc_codes", '') AS "cpc_codes",
    COALESCE(fipc."ipc_codes", '') AS "ipc_codes",
    COALESCE(ci."citing_families", '') AS "citing_families",
    COALESCE(cdi."cited_families", '') AS "cited_families"
FROM "family_earliest" fe
LEFT JOIN "family_pubs" fp ON fe."family_id" = fp."family_id"
LEFT JOIN "family_cpc" fcpc ON fe."family_id" = fcpc."family_id"
LEFT JOIN "family_ipc" fipc ON fe."family_id" = fipc."family_id"
LEFT JOIN "citing_info" ci ON fe."family_id" = ci."cited_family_id"
LEFT JOIN "cited_info" cdi ON fe."family_id" = cdi."citing_family_id"
ORDER BY fe."family_id"