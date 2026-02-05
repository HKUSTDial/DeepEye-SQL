WITH joined_data AS (
  SELECT 
    f."path",
    f."repo_name",
    f."ref",
    c."content",
    c."binary"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_FILES" f
  JOIN "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS" c
    ON f."path" = c."sample_path"
    AND f."repo_name" = c."sample_repo_name"
    AND f."ref" = c."sample_ref"
  WHERE (f."path" LIKE '%.py' OR f."path" LIKE '%.r')
    AND c."binary" = FALSE
),
lines_data AS (
  SELECT
    "path",
    "repo_name",
    "content",
    "binary",
    line.value AS line_text
  FROM joined_data,
  LATERAL SPLIT_TO_TABLE(joined_data."content", '\n') AS line
),
extracted_modules AS (
  SELECT
    'Python' AS language,
    COALESCE(
      REGEXP_SUBSTR(line_text, '^\\s*import\\s+([^\\s,;]+)', 1, 1, 'e'),
      REGEXP_SUBSTR(line_text, '^\\s*from\\s+([^\\s]+)\\s+import', 1, 1, 'e')
    ) AS module_name
  FROM lines_data
  WHERE "path" LIKE '%.py'
    AND (line_text LIKE '%import%' OR line_text LIKE '%from%import%')
  UNION ALL
  SELECT
    'R' AS language,
    REGEXP_SUBSTR(line_text, 'library\\(([^)]+)\\)', 1, 1, 'e') AS module_name
  FROM lines_data
  WHERE "path" LIKE '%.r'
    AND line_text LIKE '%library(%'
)
SELECT
  language,
  module_name,
  COUNT(*) AS occurrences
FROM extracted_modules
WHERE module_name IS NOT NULL
GROUP BY language, module_name
ORDER BY language, occurrences DESC