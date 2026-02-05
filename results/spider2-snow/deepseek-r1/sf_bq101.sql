SELECT
    REGEXP_SUBSTR(
        TRIM(REGEXP_SUBSTR("line_tbl"."VALUE", '^\\s*import\\s+(static\\s+)?([^;]+);', 1, 1, 'e', 2)),
        '^([a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)*)',
        1, 1, 'e', 1
    ) AS "package_name",
    COUNT(*) AS "count"
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS",
LATERAL SPLIT_TO_TABLE("content", '\n') AS "line_tbl"
WHERE "binary" = FALSE
    AND "sample_path" LIKE '%.java'
    AND REGEXP_LIKE("line_tbl"."VALUE", '^\\s*import\\s+(static\\s+)?[^;]+;')
GROUP BY "package_name"
HAVING "package_name" IS NOT NULL
ORDER BY "count" DESC
LIMIT 10