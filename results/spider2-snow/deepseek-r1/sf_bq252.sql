SELECT f."repo_name"
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS" c
JOIN "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_FILES" f ON c."id" = f."id"
WHERE c."binary" = false AND ENDSWITH(f."path", '.swift')
ORDER BY c."copies" DESC
LIMIT 1