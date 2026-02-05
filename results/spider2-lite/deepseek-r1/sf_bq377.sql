WITH json_data AS (
  SELECT TRY_PARSE_JSON("content") AS json_content
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE "binary" = FALSE
),
require_sections AS (
  SELECT json_content:require AS require_obj
  FROM json_data
  WHERE IS_OBJECT(require_obj)
),
package_names AS (
  SELECT f.key AS "package_name"
  FROM require_sections
  JOIN LATERAL FLATTEN(INPUT => require_obj) f
)
SELECT "package_name", COUNT(*) AS "frequency"
FROM package_names
GROUP BY "package_name"
ORDER BY "frequency" DESC