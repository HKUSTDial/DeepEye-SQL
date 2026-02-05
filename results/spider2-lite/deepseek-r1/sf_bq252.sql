SELECT "sample_repo_name"
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
WHERE "binary" = FALSE
  AND "sample_path" ILIKE '%.swift'
  AND "copies" = (
      SELECT MAX("copies")
      FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
      WHERE "binary" = FALSE
        AND "sample_path" ILIKE '%.swift'
  )
ORDER BY "sample_repo_name"
LIMIT 1