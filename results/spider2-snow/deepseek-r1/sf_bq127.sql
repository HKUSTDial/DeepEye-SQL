WITH family_earliest AS (
  SELECT 
    "family_id",
    MIN("publication_date") AS earliest_publication_date
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS"
  GROUP BY "family_id"
  HAVING earliest_publication_date BETWEEN 20150101 AND 20150131
),
family_publications AS (
  SELECT 
    fe."family_id",
    fe.earliest_publication_date,
    p."publication_number",
    p."country_code",
    p."cpc",
    p."ipc"
  FROM family_earliest fe
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
    ON fe."family_id" = p."family_id"
),
pub_agg AS (
  SELECT 
    "family_id",
    earliest_publication_date,
    LISTAGG(DISTINCT "publication_number", ',') WITHIN GROUP (ORDER BY "publication_number") AS publication_numbers,
    LISTAGG(DISTINCT "country_code", ',') WITHIN GROUP (ORDER BY "country_code") AS country_codes
  FROM family_publications
  GROUP BY "family_id", earliest_publication_date
),
cpc_codes AS (
  SELECT 
    fp."family_id",
    TRIM(fl.value::STRING) AS cpc_code
  FROM family_publications fp,
  LATERAL FLATTEN(INPUT => fp."cpc") fl
  WHERE cpc_code != ''
),
cpc_agg AS (
  SELECT 
    "family_id",
    LISTAGG(DISTINCT cpc_code, ',') WITHIN GROUP (ORDER BY cpc_code) AS cpc_codes_list
  FROM cpc_codes
  GROUP BY "family_id"
),
ipc_codes AS (
  SELECT 
    fp."family_id",
    TRIM(fl.value::STRING) AS ipc_code
  FROM family_publications fp,
  LATERAL FLATTEN(INPUT => fp."ipc") fl
  WHERE ipc_code != ''
),
ipc_agg AS (
  SELECT 
    "family_id",
    LISTAGG(DISTINCT ipc_code, ',') WITHIN GROUP (ORDER BY ipc_code) AS ipc_codes_list
  FROM ipc_codes
  GROUP BY "family_id"
),
citing_relationships AS (
  SELECT 
    a."publication_number" AS cited_pub,
    fl.value::STRING AS citing_pub
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" a,
  LATERAL FLATTEN(INPUT => a."cited_by") fl
),
citing_families_raw AS (
  SELECT 
    p_cited."family_id" AS target_family_id,
    p_citing."family_id" AS citing_family_id
  FROM citing_relationships cr
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p_cited
    ON cr.cited_pub = p_cited."publication_number"
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p_citing
    ON cr.citing_pub = p_citing."publication_number"
  WHERE p_cited."family_id" IN (SELECT "family_id" FROM family_earliest)
),
citing_families_agg AS (
  SELECT 
    target_family_id AS "family_id",
    LISTAGG(DISTINCT citing_family_id, ',') WITHIN GROUP (ORDER BY citing_family_id) AS citing_families
  FROM citing_families_raw
  GROUP BY target_family_id
),
cited_relationships AS (
  SELECT 
    p."family_id" AS citing_family_id,
    fl.value::STRING AS cited_pub
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p,
  LATERAL FLATTEN(INPUT => p."citation") fl
  WHERE p."family_id" IN (SELECT "family_id" FROM family_earliest)
),
cited_families_raw AS (
  SELECT 
    cr.citing_family_id AS target_family_id,
    p_cited."family_id" AS cited_family_id
  FROM cited_relationships cr
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p_cited
    ON cr.cited_pub = p_cited."publication_number"
),
cited_families_agg AS (
  SELECT 
    target_family_id AS "family_id",
    LISTAGG(DISTINCT cited_family_id, ',') WITHIN GROUP (ORDER BY cited_family_id) AS cited_families
  FROM cited_families_raw
  GROUP BY target_family_id
)
SELECT 
  fe."family_id",
  fe.earliest_publication_date,
  COALESCE(pub.publication_numbers, '') AS publication_numbers,
  COALESCE(pub.country_codes, '') AS country_codes,
  COALESCE(cpc.cpc_codes_list, '') AS cpc_codes,
  COALESCE(ipc.ipc_codes_list, '') AS ipc_codes,
  COALESCE(cfa.citing_families, '') AS citing_families,
  COALESCE(cfd.cited_families, '') AS cited_families
FROM family_earliest fe
LEFT JOIN pub_agg pub ON fe."family_id" = pub."family_id"
LEFT JOIN cpc_agg cpc ON fe."family_id" = cpc."family_id"
LEFT JOIN ipc_agg ipc ON fe."family_id" = ipc."family_id"
LEFT JOIN citing_families_agg cfa ON fe."family_id" = cfa."family_id"
LEFT JOIN cited_families_agg cfd ON fe."family_id" = cfd."family_id"
ORDER BY fe."family_id"