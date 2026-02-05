WITH readme_lines AS (
  SELECT
    "sample_repo_name" AS repo,
    TRIM(line.value) AS line_text
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS",
    LATERAL SPLIT_TO_TABLE("content", '\n') AS line
  WHERE "binary" = FALSE
    AND "sample_path" ILIKE '%README.md'
    AND TRIM(line.value) != ''
    AND NOT (STARTSWITH(LTRIM(line.value), '#') OR STARTSWITH(LTRIM(line.value), '//'))
),
line_freq AS (
  SELECT line_text, COUNT(*) AS frequency
  FROM readme_lines
  GROUP BY line_text
),
line_repo AS (
  SELECT DISTINCT line_text, repo
  FROM readme_lines
),
repo_langs AS (
  SELECT
    "repo_name",
    lang.value::STRING AS language
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES",
    LATERAL FLATTEN(INPUT => "language") AS lang
),
line_langs AS (
  SELECT lr.line_text, rl.language
  FROM line_repo lr
  LEFT JOIN repo_langs rl ON lr.repo = rl."repo_name"
  WHERE rl.language IS NOT NULL
),
line_lang_list AS (
  SELECT
    line_text,
    LISTAGG(language, ', ') WITHIN GROUP (ORDER BY language) AS languages
  FROM line_langs
  GROUP BY line_text
)
SELECT
  f.line_text,
  f.frequency,
  COALESCE(l.languages, '') AS languages
FROM line_freq f
LEFT JOIN line_lang_list l ON f.line_text = l.line_text
ORDER BY f.frequency DESC