WITH all_lines AS (
  SELECT RTRIM(s.value, '\r') AS line_text
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS",
  LATERAL SPLIT_TO_TABLE("content", '\n') AS s
  WHERE "binary" = FALSE
),
categorized AS (
  SELECT 
    CASE 
      WHEN RIGHT(line_text, 1) IN (' ', '\t') THEN 'trailing'
      WHEN LEFT(line_text, 1) = ' ' THEN 'Space'
      ELSE 'Other'
    END AS category
  FROM all_lines
)
SELECT category, COUNT(*) AS occurrences
FROM categorized
GROUP BY category
ORDER BY category