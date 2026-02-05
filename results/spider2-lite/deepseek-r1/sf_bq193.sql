WITH lines AS (
    SELECT 
        contents."sample_repo_name" AS repo,
        TRIM(line.value, ' \t\r\n') AS line_text
    FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS" AS contents,
    LATERAL SPLIT_TO_TABLE(contents."content", CHAR(10)) AS line
    WHERE contents."sample_path" ILIKE '%README.md'
        AND contents."binary" = FALSE
        AND TRIM(line.value, ' \t\r\n') != ''
        AND NOT (TRIM(line.value, ' \t\r\n') LIKE '#%' OR TRIM(line.value, ' \t\r\n') LIKE '//%')
), line_freq AS (
    SELECT 
        line_text,
        COUNT(*) AS frequency
    FROM lines
    GROUP BY line_text
), line_repos AS (
    SELECT DISTINCT
        line_text,
        repo
    FROM lines
), repo_languages AS (
    SELECT 
        lang_table."repo_name",
        lang.value AS language
    FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES" AS lang_table,
    LATERAL FLATTEN(INPUT => lang_table."language") AS lang
), line_languages AS (
    SELECT 
        lr.line_text,
        LISTAGG(DISTINCT rl.language, ',') WITHIN GROUP (ORDER BY rl.language) AS languages
    FROM line_repos lr
    JOIN repo_languages rl ON lr.repo = rl."repo_name"
    GROUP BY lr.line_text
)
SELECT 
    f.line_text,
    f.frequency,
    COALESCE(ll.languages, '') AS languages
FROM line_freq f
LEFT JOIN line_languages ll ON f.line_text = ll.line_text
ORDER BY f.frequency DESC