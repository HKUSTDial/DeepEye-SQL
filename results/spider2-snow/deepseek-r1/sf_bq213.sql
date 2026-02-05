WITH patent_ipc AS (
    SELECT p."publication_number",
           SUBSTR(flattened_ipc.value:"code"::STRING, 1, 4) AS ipc4,
           flattened_ipc.value:"first"::BOOLEAN AS is_first
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" p
    , LATERAL FLATTEN(INPUT => p."ipc") AS flattened_ipc
    WHERE p."country_code" = 'US'
      AND p."kind_code" = 'B2'
      AND p."grant_date" BETWEEN 20220601 AND 20220831
      AND flattened_ipc.value:"code" IS NOT NULL
),
ipc_counts_per_patent AS (
    SELECT "publication_number",
           ipc4,
           COUNT(*) AS cnt,
           MAX(CASE WHEN is_first THEN 1 ELSE 0 END) AS has_first
    FROM patent_ipc
    GROUP BY "publication_number", ipc4
),
ranked_ipc AS (
    SELECT "publication_number",
           ipc4,
           cnt,
           has_first,
           ROW_NUMBER() OVER (PARTITION BY "publication_number" ORDER BY has_first DESC, cnt DESC, ipc4 ASC) AS rn
    FROM ipc_counts_per_patent
),
selected_ipc AS (
    SELECT "publication_number", ipc4
    FROM ranked_ipc
    WHERE rn = 1
),
ipc_aggregate AS (
    SELECT ipc4,
           COUNT(*) AS patent_count
    FROM selected_ipc
    GROUP BY ipc4
)
SELECT ipc4, patent_count
FROM ipc_aggregate
ORDER BY patent_count DESC
LIMIT 1