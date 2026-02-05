WITH lines AS (
    SELECT
        "sample_path",
        line.value AS line_text
    FROM
        "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
    CROSS JOIN
        LATERAL FLATTEN(INPUT => SPLIT("content", '\n')) AS line
    WHERE
        (LOWER("sample_path") LIKE '%.py' OR LOWER("sample_path") LIKE '%.r')
        AND "content" IS NOT NULL
),
python_imports AS (
    SELECT
        REGEXP_SUBSTR(line_text, '^[[:space:]]*import[[:space:]]+([a-zA-Z0-9_.]+)') AS module_name
    FROM
        lines
    WHERE
        LOWER("sample_path") LIKE '%.py'
        AND REGEXP_LIKE(line_text, '^[[:space:]]*import[[:space:]]+[a-zA-Z0-9_.]+')
),
python_from_modules AS (
    SELECT
        REGEXP_SUBSTR(line_text, '^[[:space:]]*from[[:space:]]+([a-zA-Z0-9_.]+)[[:space:]]+import') AS module_name
    FROM
        lines
    WHERE
        LOWER("sample_path") LIKE '%.py'
        AND REGEXP_LIKE(line_text, '^[[:space:]]*from[[:space:]]+[a-zA-Z0-9_.]+[[:space:]]+import')
),
r_library_modules AS (
    SELECT
        REGEXP_SUBSTR(line_text, 'library\\([[:space:]]*[\'"]?([a-zA-Z0-9_.]+)[\'"]?[[:space:]]*\\)') AS module_name
    FROM
        lines
    WHERE
        LOWER("sample_path") LIKE '%.r'
        AND REGEXP_LIKE(line_text, 'library\\([[:space:]]*[\'"]?[a-zA-Z0-9_.]+[\'"]?[[:space:]]*\\)')
),
all_modules AS (
    SELECT module_name FROM python_imports
    UNION ALL
    SELECT module_name FROM python_from_modules
    UNION ALL
    SELECT module_name FROM r_library_modules
)
SELECT
    module_name,
    COUNT(*) AS frequency
FROM
    all_modules
WHERE
    module_name IS NOT NULL
GROUP BY
    module_name
ORDER BY
    frequency DESC
LIMIT 5