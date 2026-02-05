WITH valid_patents AS (
  SELECT 
    "publication_number",
    CAST(SUBSTRING(CAST("filing_date" AS VARCHAR(8)), 1, 4) AS INTEGER) AS "year",
    "cpc"[0]::STRING AS "first_cpc"
  FROM "PATENTS"."PATENTS"."PUBLICATIONS"
  WHERE "filing_date" IS NOT NULL
    AND "application_number" IS NOT NULL
    AND "application_number" != ''
    AND "cpc"[0]::STRING IS NOT NULL
),
detailed_cpc AS (
  SELECT vp."publication_number", vp."year", vp."first_cpc", cd."parents" AS "parents_array"
  FROM valid_patents vp
  JOIN "PATENTS"."PATENTS"."CPC_DEFINITION" cd ON vp."first_cpc" = cd."symbol"
),
all_ancestors AS (
  SELECT dc."publication_number", dc."year", dc."first_cpc", f.value::STRING AS "ancestor_symbol"
  FROM detailed_cpc dc,
  LATERAL FLATTEN(INPUT => dc."parents_array") f
  UNION ALL
  SELECT "publication_number", "year", "first_cpc", "first_cpc" AS "ancestor_symbol"
  FROM detailed_cpc
),
level5_mapping AS (
  SELECT DISTINCT aa."publication_number", cd."symbol" AS "level5_symbol", cd."titleFull" AS "level5_title"
  FROM all_ancestors aa
  JOIN "PATENTS"."PATENTS"."CPC_DEFINITION" cd ON aa."ancestor_symbol" = cd."symbol" AND cd."level" = 5
),
grouped_counts AS (
  SELECT lm."level5_symbol", lm."level5_title", vp."year", COUNT(*) AS "filing_count"
  FROM valid_patents vp
  JOIN level5_mapping lm ON vp."publication_number" = lm."publication_number"
  GROUP BY lm."level5_symbol", lm."level5_title", vp."year"
),
ordered_counts AS (
  SELECT "level5_symbol", "level5_title", "year", "filing_count",
         ROW_NUMBER() OVER (PARTITION BY "level5_symbol" ORDER BY "year") AS "rn"
  FROM grouped_counts
),
recursive_ema AS (
  SELECT "level5_symbol", "level5_title", "year", "filing_count", "filing_count" AS "ema", "rn"
  FROM ordered_counts
  WHERE "rn" = 1
  UNION ALL
  SELECT oc."level5_symbol", oc."level5_title", oc."year", oc."filing_count",
         0.2 * oc."filing_count" + 0.8 * re."ema" AS "ema",
         oc."rn"
  FROM ordered_counts oc
  JOIN recursive_ema re ON oc."level5_symbol" = re."level5_symbol" AND oc."rn" = re."rn" + 1
),
ranked_ema AS (
  SELECT "level5_symbol", "level5_title", "year", "ema",
         ROW_NUMBER() OVER (PARTITION BY "level5_symbol" ORDER BY "ema" DESC, "year" DESC) AS "rank"
  FROM recursive_ema
)
SELECT "level5_title", "year" AS "best_year", "ema" AS "highest_exponential_moving_average"
FROM ranked_ema
WHERE "rank" = 1
ORDER BY "level5_title"