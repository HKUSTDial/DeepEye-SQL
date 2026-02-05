WITH psoriasis_disease AS (
  SELECT "id" FROM "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."DISEASES" WHERE LOWER("name") = 'psoriasis'
),
association_scores AS (
  SELECT "targetId", "score" FROM "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."ASSOCIATIONBYOVERALLDIRECT" WHERE "diseaseId" = (SELECT "id" FROM psoriasis_disease)
),
mean_score AS (
  SELECT AVG("score") AS mean_val FROM association_scores
),
differences AS (
  SELECT a."targetId", a."score", ABS(a."score" - m.mean_val) AS diff FROM association_scores a CROSS JOIN mean_score m
),
closest_target AS (
  SELECT "targetId" FROM differences ORDER BY diff, "targetId" LIMIT 1
)
SELECT t."approvedSymbol" FROM "OPEN_TARGETS_PLATFORM_1"."PLATFORM"."TARGETS" t WHERE t."id" = (SELECT "targetId" FROM closest_target)