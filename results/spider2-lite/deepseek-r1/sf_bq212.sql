WITH filtered_patents AS (
  SELECT 
    "publication_number",
    "ipc"
  FROM 
    "PATENTS"."PATENTS"."PUBLICATIONS"
  WHERE 
    "country_code" = 'US'
    AND "kind_code" = 'B2'
    AND "grant_date" BETWEEN 20220601 AND 20220930
),
flattened_ipc AS (
  SELECT 
    fp."publication_number",
    SUBSTR(ipc_entry.value['code']::STRING, 1, 4) AS ipc4
  FROM 
    filtered_patents fp,
    LATERAL FLATTEN(INPUT => fp."ipc") AS ipc_entry
  WHERE 
    ipc_entry.value['code']::STRING IS NOT NULL
    AND LENGTH(ipc_entry.value['code']::STRING) >= 4
),
ipc_counts AS (
  SELECT 
    "publication_number",
    ipc4,
    COUNT(*) AS ipc4_count
  FROM 
    flattened_ipc
  GROUP BY 
    "publication_number",
    ipc4
),
ranked_ipc AS (
  SELECT 
    "publication_number",
    ipc4,
    ipc4_count,
    ROW_NUMBER() OVER (PARTITION BY "publication_number" ORDER BY ipc4_count DESC) AS rn
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
ORDER BY 
  "publication_number"