SELECT title
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE CONTAINS_SUBSTR(tags, 'android-') AND CONTAINS_SUBSTR(title, 'how')
ORDER BY view_count DESC
LIMIT 1