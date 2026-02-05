WITH python_imports AS (
  SELECT 
    REGEXP_SUBSTR("content", 'import\\s+([a-zA-Z0-9_\\.]+)', 1, 1, 'e') as library_name
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE 
    ("sample_path" LIKE '%.py' OR "sample_path" LIKE '%.ipynb')
    AND "binary" = FALSE
    AND REGEXP_SUBSTR("content", 'import\\s+([a-zA-Z0-9_\\.]+)', 1, 1, 'e') IS NOT NULL
),
python_from_imports AS (
  SELECT 
    REGEXP_SUBSTR("content", 'from\\s+([a-zA-Z0-9_\\.]+)\\s+import', 1, 1, 'e') as library_name
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE 
    ("sample_path" LIKE '%.py' OR "sample_path" LIKE '%.ipynb')
    AND "binary" = FALSE
    AND REGEXP_SUBSTR("content", 'from\\s+([a-zA-Z0-9_\\.]+)\\s+import', 1, 1, 'e') IS NOT NULL
),
r_library_calls AS (
  SELECT 
    REGEXP_SUBSTR("content", 'library\\(["'']?([a-zA-Z0-9_\\.]+)["'']?\\)', 1, 1, 'e') as library_name
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE 
    ("sample_path" LIKE '%.r' 
     OR "sample_path" LIKE '%.R'
     OR "sample_path" LIKE '%.Rmd'
     OR "sample_path" LIKE '%.rmd')
    AND "binary" = FALSE
    AND REGEXP_SUBSTR("content", 'library\\(["'']?([a-zA-Z0-9_\\.]+)["'']?\\)', 1, 1, 'e') IS NOT NULL
),
r_require_calls AS (
  SELECT 
    REGEXP_SUBSTR("content", 'require\\(["'']?([a-zA-Z0-9_\\.]+)["'']?\\)', 1, 1, 'e') as library_name
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE 
    ("sample_path" LIKE '%.r' 
     OR "sample_path" LIKE '%.R'
     OR "sample_path" LIKE '%.Rmd'
     OR "sample_path" LIKE '%.rmd')
    AND "binary" = FALSE
    AND REGEXP_SUBSTR("content", 'require\\(["'']?([a-zA-Z0-9_\\.]+)["'']?\\)', 1, 1, 'e') IS NOT NULL
),
all_imports AS (
  SELECT library_name FROM python_imports
  UNION ALL
  SELECT library_name FROM python_from_imports
  UNION ALL
  SELECT library_name FROM r_library_calls
  UNION ALL
  SELECT library_name FROM r_require_calls
),
ranked_imports AS (
  SELECT 
    library_name,
    COUNT(*) as import_count,
    RANK() OVER (ORDER BY COUNT(*) DESC) as rank_num
  FROM all_imports
  WHERE library_name IS NOT NULL
  GROUP BY library_name
)
SELECT library_name, import_count
FROM ranked_imports
WHERE rank_num = 2