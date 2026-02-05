WITH relevant_files AS (
  SELECT 
    f."id",
    f."path",
    c."content"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_FILES" f
  JOIN "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS" c ON f."id" = c."id"
  WHERE (
    LOWER(f."path") LIKE '%.py' 
    OR LOWER(f."path") LIKE '%.r' 
    OR LOWER(f."path") LIKE '%.rmd' 
    OR LOWER(f."path") LIKE '%.r' 
    OR LOWER(f."path") LIKE '%.rmd' 
    OR LOWER(f."path") LIKE '%.ipynb'
  )
  AND c."binary" = FALSE
),
python_imports AS (
  SELECT 
    REGEXP_SUBSTR(
      REGEXP_SUBSTR("content", 'import[[:space:]]+([a-zA-Z0-9_]+)', 1, 1, 'i'),
      '[a-zA-Z0-9_]+'
    ) as module_name
  FROM relevant_files
  WHERE LOWER("path") LIKE '%.py'
  AND "content" LIKE '%import%'
  UNION ALL
  SELECT 
    REGEXP_SUBSTR(
      REGEXP_SUBSTR("content", 'from[[:space:]]+([a-zA-Z0-9_.]+)[[:space:]]+import', 1, 1, 'i'),
      '^[a-zA-Z0-9_]+'
    ) as module_name
  FROM relevant_files
  WHERE LOWER("path") LIKE '%.py'
  AND "content" LIKE '%from%'
),
r_imports AS (
  SELECT 
    REGEXP_SUBSTR(
      REGEXP_SUBSTR("content", 'library\\(["\']?([a-zA-Z0-9_.]+)["\']?\\)', 1, 1, 'i'),
      '[a-zA-Z0-9_.]+'
    ) as module_name
  FROM relevant_files
  WHERE (LOWER("path") LIKE '%.r' OR LOWER("path") LIKE '%.rmd')
  AND "content" LIKE '%library(%'
  UNION ALL
  SELECT 
    REGEXP_SUBSTR(
      REGEXP_SUBSTR("content", 'require\\(["\']?([a-zA-Z0-9_.]+)["\']?\\)', 1, 1, 'i'),
      '[a-zA-Z0-9_.]+'
    ) as module_name
  FROM relevant_files
  WHERE (LOWER("path") LIKE '%.r' OR LOWER("path") LIKE '%.rmd')
  AND "content" LIKE '%require(%'
),
all_imports AS (
  SELECT module_name FROM python_imports WHERE module_name IS NOT NULL
  UNION ALL
  SELECT module_name FROM r_imports WHERE module_name IS NOT NULL
),
import_counts AS (
  SELECT 
    module_name,
    COUNT(*) as import_count,
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
  FROM all_imports
  GROUP BY module_name
)
SELECT module_name
FROM import_counts
WHERE rank = 2