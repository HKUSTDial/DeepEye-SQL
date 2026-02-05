WITH target AS (
  SELECT p."publication_number", FLOOR(p."filing_date"/10000) AS filing_year, a."embedding_v1"
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" a ON p."publication_number" = a."publication_number"
  WHERE p."publication_number" = 'US-9741766-B2'
),
candidates AS (
  SELECT p."publication_number", FLOOR(p."filing_date"/10000) AS filing_year, a."embedding_v1"
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" a ON p."publication_number" = a."publication_number"
  WHERE FLOOR(p."filing_date"/10000) = (SELECT filing_year FROM target)
    AND p."publication_number" != 'US-9741766-B2'
),
target_flattened AS (
  SELECT t."publication_number", f.index, f.value AS val
  FROM target t,
  LATERAL FLATTEN(input => t."embedding_v1") f
),
candidate_flattened AS (
  SELECT c."publication_number", f.index, f.value AS val
  FROM candidates c,
  LATERAL FLATTEN(input => c."embedding_v1") f
)
SELECT cf."publication_number"
FROM target_flattened tf
JOIN candidate_flattened cf ON tf.index = cf.index
GROUP BY cf."publication_number"
ORDER BY SUM(tf.val * cf.val) DESC
LIMIT 5