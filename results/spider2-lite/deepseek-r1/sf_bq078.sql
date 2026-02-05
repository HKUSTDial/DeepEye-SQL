WITH combined_associations AS (
  SELECT "targetId", "score"
  FROM "OPEN_TARGETS_PLATFORM_2"."OPEN_TARGETS_PLATFORM"."ASSOCIATIONBYDATASOURCEDIRECT"
  WHERE "diseaseId" = 'EFO_0000676' AND "datasourceId" = 'impc'
  UNION ALL
  SELECT "targetId", "score"
  FROM "OPEN_TARGETS_PLATFORM_2"."OPEN_TARGETS_PLATFORM"."ASSOCIATIONBYDATASOURCEINDIRECT"
  WHERE "diseaseId" = 'EFO_0000676' AND "datasourceId" = 'impc'
),
max_score AS (
  SELECT MAX("score") AS max_score
  FROM combined_associations
)
SELECT DISTINCT t."approvedSymbol"
FROM combined_associations ca
JOIN max_score ms ON ca."score" = ms.max_score
JOIN "OPEN_TARGETS_PLATFORM_2"."OPEN_TARGETS_PLATFORM"."TARGETS" t ON ca."targetId" = t."id"