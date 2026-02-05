WITH python_modules AS (
  SELECT 
    REGEXP_SUBSTR_ALL(
      "content", 
      '^\\s*(import|from)\\s+([a-zA-Z0-9_.]+)',
      1,
      1,
      'm',
      2
    ) AS module_names,
    "copies"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE ENDSWITH("sample_path", '.py')
    AND "binary" = FALSE
),
r_modules AS (
  SELECT 
    REGEXP_SUBSTR_ALL(
      "content", 
      'library\\(["'']?([a-zA-Z0-9_.]+)["'']?\\)',
      1,
      1,
      'c',
      1
    ) AS module_names,
    "copies"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE (ENDSWITH("sample_path", '.r') OR ENDSWITH("sample_path", '.R'))
    AND "binary" = FALSE
),
all_modules AS (
  SELECT f.value AS module_name, pm."copies"
  FROM python_modules pm,
  LATERAL FLATTEN(INPUT => pm.module_names) AS f
  WHERE f.value IS NOT NULL
  UNION ALL
  SELECT f.value AS module_name, rm."copies"
  FROM r_modules rm,
  LATERAL FLATTEN(INPUT => rm.module_names) AS f
  WHERE f.value IS NOT NULL
)
SELECT 
  module_name,
  SUM("copies") AS frequency
FROM all_modules
GROUP BY module_name
ORDER BY frequency DESC
LIMIT 5