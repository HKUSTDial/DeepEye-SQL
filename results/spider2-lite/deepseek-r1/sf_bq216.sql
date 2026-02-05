WITH target_year AS (
  SELECT FLOOR("filing_date" / 10000) AS year
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS"
  WHERE "publication_number" = 'US-9741766-B2'
), target_embedding AS (
  SELECT "embedding_v1"
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB"
  WHERE "publication_number" = 'US-9741766-B2'
), candidates AS (
  SELECT p."publication_number"
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
  CROSS JOIN target_year ty
  WHERE FLOOR(p."filing_date" / 10000) = ty.year
    AND p."publication_number" != 'US-9741766-B2'
), candidate_embeddings AS (
  SELECT c."publication_number", a."embedding_v1"
  FROM candidates c
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" a ON c."publication_number" = a."publication_number"
), similarity AS (
  SELECT ce."publication_number", SUM(te_val.value::FLOAT * ce_val.value::FLOAT) AS similarity_score
  FROM target_embedding te
  CROSS JOIN candidate_embeddings ce
  , LATERAL FLATTEN(INPUT => te."embedding_v1") te_val
  , LATERAL FLATTEN(INPUT => ce."embedding_v1") ce_val
  WHERE te_val.index = ce_val.index
  GROUP BY ce."publication_number"
)
SELECT "publication_number"
FROM similarity
ORDER BY similarity_score DESC
LIMIT 5