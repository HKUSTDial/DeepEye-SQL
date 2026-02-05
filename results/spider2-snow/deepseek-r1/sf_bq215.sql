WITH focal_patents AS (
  SELECT "publication_number"
  FROM "PATENTS"."PATENTS"."PUBLICATIONS"
  WHERE "country_code" = 'US'
    AND "kind_code" = 'B2'
    AND "grant_date" BETWEEN 20150101 AND 20181231
),
citations AS (
  SELECT
    fp."publication_number" AS focal_patent,
    citation_u.value:"publication_number"::STRING AS cited_pub
  FROM focal_patents fp
  JOIN "PATENTS"."PATENTS"."PUBLICATIONS" p
    ON fp."publication_number" = p."publication_number"
  LEFT JOIN LATERAL FLATTEN(INPUT => p."citation") citation_u
),
cited_ipc AS (
  SELECT
    "publication_number",
    SUBSTR(ipc_u.value:"code"::STRING, 1, 4) AS ipc4,
    COUNT(*) AS cnt
  FROM "PATENTS"."PATENTS"."PUBLICATIONS"
  LEFT JOIN LATERAL FLATTEN(INPUT => "ipc") ipc_u
  WHERE ipc_u.value:"code" IS NOT NULL
  GROUP BY "publication_number", ipc4
),
ranked_ipc AS (
  SELECT
    "publication_number",
    ipc4,
    ROW_NUMBER() OVER (PARTITION BY "publication_number" ORDER BY cnt DESC) AS rn
  FROM cited_ipc
),
representative_ipc AS (
  SELECT "publication_number", ipc4
  FROM ranked_ipc
  WHERE rn = 1
),
citations_with_ipc AS (
  SELECT
    c.focal_patent,
    COALESCE(ripc.ipc4, 'N/A') AS ipc4
  FROM citations c
  LEFT JOIN representative_ipc ripc
    ON c.cited_pub = ripc."publication_number"
  WHERE c.cited_pub IS NOT NULL
),
ipc_occurrences AS (
  SELECT
    focal_patent,
    ipc4,
    COUNT(*) AS occurrences
  FROM citations_with_ipc
  WHERE ipc4 != 'N/A'
  GROUP BY focal_patent, ipc4
),
originality_scores AS (
  SELECT
    focal_patent,
    1.0 - (SUM(occurrences * occurrences) / (SUM(occurrences) * SUM(occurrences))) AS originality
  FROM ipc_occurrences
  GROUP BY focal_patent
  HAVING SUM(occurrences) > 0
)
SELECT focal_patent, originality
FROM originality_scores
ORDER BY originality DESC
LIMIT 1