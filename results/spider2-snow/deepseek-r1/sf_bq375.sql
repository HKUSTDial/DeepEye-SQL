WITH file_details AS (
    SELECT 
        "path",
        (LENGTH("path") - LENGTH(REPLACE("path", '/', ''))) AS depth,
        LOWER(SPLIT_PART("path", '.', -1)) AS extension
    FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_FILES"
)
SELECT 
    extension,
    COUNT(*) AS file_count
FROM file_details
WHERE depth > 10
    AND extension IN ('py', 'c', 'ipynb', 'java', 'js')
GROUP BY extension
ORDER BY file_count DESC
LIMIT 1