WITH collisions_with_year AS (
  SELECT 
    c."pcf_violation_category" AS cause,
    ci."db_year" AS year
  FROM "CALIFORNIA_TRAFFIC_COLLISION"."CALIFORNIA_TRAFFIC_COLLISION"."COLLISIONS" c
  INNER JOIN "CALIFORNIA_TRAFFIC_COLLISION"."CALIFORNIA_TRAFFIC_COLLISION"."CASE_IDS" ci
    ON c."case_id" = ci."case_id"
  WHERE c."pcf_violation_category" IS NOT NULL
),
counts_per_cause_per_year AS (
  SELECT 
    year,
    cause,
    COUNT(*) AS accident_count
  FROM collisions_with_year
  GROUP BY year, cause
),
ranked_causes AS (
  SELECT 
    year,
    cause,
    accident_count,
    ROW_NUMBER() OVER (PARTITION BY year ORDER BY accident_count DESC, cause) AS rn
  FROM counts_per_cause_per_year
),
top_two_per_year AS (
  SELECT 
    year,
    cause,
    rn
  FROM ranked_causes
  WHERE rn <= 2
),
aggregated_top_two AS (
  SELECT 
    year,
    LISTAGG(cause, ', ') WITHIN GROUP (ORDER BY cause) AS top_two_set
  FROM top_two_per_year
  GROUP BY year
  HAVING COUNT(*) = 2
),
uniqueness AS (
  SELECT 
    top_two_set,
    COUNT(*) AS year_count
  FROM aggregated_top_two
  GROUP BY top_two_set
)
SELECT 
  a.year
FROM aggregated_top_two a
JOIN uniqueness u ON a.top_two_set = u.top_two_set
WHERE u.year_count = 1
ORDER BY a.year