SELECT f.key AS package_name, COUNT(*) AS frequency
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS",
LATERAL FLATTEN(INPUT => TRY_PARSE_JSON("content"):require) f
WHERE "content" IS NOT NULL
AND IS_OBJECT(TRY_PARSE_JSON("content"):require)
GROUP BY f.key
ORDER BY frequency DESC