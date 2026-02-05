WITH ipc_counts AS (
  SELECT
    p."publication_number",
    SUBSTR(ipc_flattened.value:code::text, 1, 4) AS ipc4,
    COUNT(*) AS ipc4_count
  FROM
    "PATENTS"."PATENTS"."PUBLICATIONS" p,
    LATERAL FLATTEN(INPUT => p."ipc") ipc_flattened
  WHERE
    p."country_code" = 'US'
    AND p."application_kind" = 'U'
    AND p."kind_code" = 'B2'
    AND p."grant_date" BETWEEN 20220601 AND 20220930
  GROUP BY
    p."publication_number",
    ipc4
),
ranked_ipc AS (
  SELECT
    "publication_number",
    ipc4,
    ipc4_count,
    ROW_NUMBER() OVER (PARTITION BY "publication_number" ORDER BY ipc4_count DESC, ipc4 ASC) AS rn
  FROM
    ipc_counts
)
SELECT
  "publication_number",
  ipc4
FROM
  ranked_ipc
WHERE
  rn = 1
  AND ipc4_count >= 10