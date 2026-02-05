WITH java_files AS (
  SELECT "content", "id"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE ENDSWITH("sample_path", '.java') AND "binary" = FALSE
),
lines AS (
  SELECT t.value AS line, jf."id"
  FROM java_files jf,
  LATERAL FLATTEN(INPUT => SPLIT(jf."content", '\n')) t
),
import_lines AS (
  SELECT line, "id"
  FROM lines
  WHERE REGEXP_LIKE(line, '^\\s*import\\s+.*;$')
),
packages AS (
  SELECT REGEXP_SUBSTR(line, '^\\s*import\\s+(static\\s+)?([^;]+)\\.[^.;]+\\s*;$', 1, 1, 'e', 2) AS package
  FROM import_lines
)
SELECT package, COUNT(*) AS frequency
FROM packages
WHERE package IS NOT NULL
GROUP BY package
ORDER BY frequency DESC
LIMIT 10