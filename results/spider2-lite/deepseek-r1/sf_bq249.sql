SELECT
  CASE
    WHEN REGEXP_LIKE(line."VALUE", '[ \t]$') THEN 'trailing'
    WHEN line."VALUE" LIKE ' %' THEN 'Space'
    ELSE 'Other'
  END AS "category",
  COUNT(*) AS "total_occurrences"
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS",
LATERAL FLATTEN(INPUT => SPLIT("content", '\n')) AS line
WHERE "binary" = FALSE
GROUP BY "category"
ORDER BY "category"