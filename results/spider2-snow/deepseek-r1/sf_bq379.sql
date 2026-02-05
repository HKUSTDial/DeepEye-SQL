WITH psoriasis_disease AS (
  SELECT DISTINCT d."id"
  FROM "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."DISEASES" d
  LEFT JOIN LATERAL FLATTEN(INPUT => d."synonyms") s
  WHERE LOWER(d."name") LIKE '%psoriasis%' OR LOWER(s.value) LIKE '%psoriasis%'
),
mean_score AS (
  SELECT AVG(a."score") AS avg_score
  FROM "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."ASSOCIATIONBYOVERALLDIRECT" a
  INNER JOIN psoriasis_disease d ON a."diseaseId" = d."id"
),
score_differences AS (
  SELECT a."targetId", a."score", ABS(a."score" - m.avg_score) AS diff
  FROM "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."ASSOCIATIONBYOVERALLDIRECT" a
  INNER JOIN psoriasis_disease d ON a."diseaseId" = d."id"
  CROSS JOIN mean_score m
),
min_difference AS (
  SELECT "targetId", diff
  FROM score_differences
  ORDER BY diff
  LIMIT 1
)
SELECT t."approvedSymbol"
FROM "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."TARGETS" t
INNER JOIN min_difference md ON t."id" = md."targetId"