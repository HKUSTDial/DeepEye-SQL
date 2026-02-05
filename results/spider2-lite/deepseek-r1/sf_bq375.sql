SELECT
    extension AS file_type,
    COUNT(*) AS file_count
FROM (
    SELECT
        REGEXP_COUNT("path", '/') AS depth,
        LOWER(SPLIT_PART("path", '.', -1)) AS extension
    FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_FILES"
) 
WHERE extension IN ('py','c','ipynb','java','js') AND depth > 10
GROUP BY extension
ORDER BY file_count DESC
LIMIT 1