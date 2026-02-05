WITH numbers AS (
  SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 30))
),
filtered_contents AS (
  SELECT "content"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE "binary" = FALSE
    AND "content" ILIKE '%import(%'
    AND REGEXP_COUNT("content", 'import\\s*\\(([\\s\\S]*?)\\)') > 0
),
import_blocks AS (
  SELECT 
    REGEXP_SUBSTR("content", 'import\\s*\\(([\\s\\S]*?)\\)', 1, numbers.n, 'e', 1) AS import_text
  FROM filtered_contents
  CROSS JOIN numbers
  WHERE numbers.n <= REGEXP_COUNT("content", 'import\\s*\\(([\\s\\S]*?)\\)')
    AND REGEXP_SUBSTR("content", 'import\\s*\\(([\\s\\S]*?)\\)', 1, numbers.n, 'e', 1) IS NOT NULL
),
split_lines AS (
  SELECT 
    TRIM(s.value) AS import_line
  FROM import_blocks,
    TABLE(SPLIT_TO_TABLE(import_blocks.import_text, '\n')) s
  WHERE import_blocks.import_text IS NOT NULL
),
package_names AS (
  SELECT 
    REGEXP_SUBSTR(import_line, '"([^"]+)"', 1, 1, 'e', 1) AS package
  FROM split_lines
  WHERE import_line IS NOT NULL
    AND LENGTH(TRIM(import_line)) > 0
)
SELECT 
  package,
  COUNT(*) AS frequency
FROM package_names
WHERE package IS NOT NULL
GROUP BY package
ORDER BY frequency DESC
LIMIT 10