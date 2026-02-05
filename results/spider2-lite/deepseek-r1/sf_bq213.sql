WITH filtered_patents AS (
    SELECT "publication_number", "grant_date", "ipc"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS"
    WHERE "country_code" = 'US'
        AND "kind_code" = 'B2'
        AND "grant_date" BETWEEN 20220601 AND 20220831
),
flattened_ipc AS (
    SELECT fp."publication_number",
           ipc_u.value:code::STRING AS ipc_code
    FROM filtered_patents fp,
    LATERAL FLATTEN(INPUT => fp."ipc") AS ipc_u
),
ipc4_counts_per_patent AS (
    SELECT "publication_number",
           SUBSTR(ipc_code, 1, 4) AS ipc4,
           COUNT(*) AS count_ipc4
    FROM flattened_ipc
    WHERE ipc_code IS NOT NULL
    GROUP BY "publication_number", ipc4
),
ranked_ipc4_per_patent AS (
    SELECT "publication_number",
           ipc4,
           ROW_NUMBER() OVER (PARTITION BY "publication_number" ORDER BY count_ipc4 DESC, ipc4 ASC) AS rn
    FROM ipc4_counts_per_patent
),
representative_ipc4 AS (
    SELECT "publication_number", ipc4
    FROM ranked_ipc4_per_patent
    WHERE rn = 1
),
ipc4_overall_counts AS (
    SELECT ipc4, COUNT(*) AS patent_count
    FROM representative_ipc4
    GROUP BY ipc4
)
SELECT ipc4, patent_count
FROM ipc4_overall_counts
ORDER BY patent_count DESC
LIMIT 1