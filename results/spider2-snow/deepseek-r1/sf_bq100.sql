WITH import_blocks AS (
  SELECT f.value AS block_text
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS",
  LATERAL FLATTEN(input => REGEXP_SUBSTR_ALL("content", 'import\\s*\\(([^)]+)\\)', 1, 1, 's', 1)) f
),
packages AS (
  SELECT TRIM(f2.value) AS package_name
  FROM import_blocks,
  LATERAL FLATTEN(input => REGEXP_SUBSTR_ALL(block_text, '"([^"]+)"', 1, 1, 's', 1)) f2
  WHERE TRIM(f2.value) IS NOT NULL AND TRIM(f2.value) != ''
)
SELECT package_name, COUNT(*) AS frequency
FROM packages
GROUP BY package_name
ORDER BY frequency DESC
LIMIT 10