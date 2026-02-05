WITH joined_files AS (
  SELECT 
    f."path",
    c."content",
    CASE 
      WHEN f."path" LIKE '%.py' THEN 'Python'
      WHEN f."path" LIKE '%.r' THEN 'R'
    END AS language
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_FILES" f
  INNER JOIN "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS" c
  ON f."id" = c."id"
  WHERE f."path" LIKE '%.py' OR f."path" LIKE '%.r'
),
lines AS (
  SELECT 
    j.language,
    j."path",
    TRIM(fl.value) AS line
  FROM joined_files j,
  LATERAL FLATTEN(INPUT => SPLIT(j."content", '\n')) fl
),
extracted AS (
  SELECT 
    language,
    CASE 
      WHEN language = 'Python' AND REGEXP_SUBSTR(line, '^\\s*import\\s+([a-zA-Z0-9_]+)', 1, 1, 'e') IS NOT NULL 
        THEN REGEXP_SUBSTR(line, '^\\s*import\\s+([a-zA-Z0-9_]+)', 1, 1, 'e')
      WHEN language = 'Python' AND REGEXP_SUBSTR(line, '^\\s*from\\s+([a-zA-Z0-9_.]+)\\s+import', 1, 1, 'e') IS NOT NULL 
        THEN REGEXP_SUBSTR(line, '^\\s*from\\s+([a-zA-Z0-9_.]+)\\s+import', 1, 1, 'e')
      WHEN language = 'R' AND REGEXP_SUBSTR(line, 'library\\s*\\(\\s*([^),]+)', 1, 1, 'e') IS NOT NULL 
        THEN REGEXP_SUBSTR(line, 'library\\s*\\(\\s*([^),]+)', 1, 1, 'e')
    END AS module
  FROM lines
  WHERE (language = 'Python' AND (line LIKE '%import%' OR line LIKE '%from%'))
     OR (language = 'R' AND line LIKE '%library(%')
)
SELECT 
  language,
  module,
  COUNT(*) AS occurrences
FROM extracted
WHERE module IS NOT NULL
GROUP BY language, module
ORDER BY language, occurrences DESC