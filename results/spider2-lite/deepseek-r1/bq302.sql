SELECT 
  FORMAT_DATE('%Y-%m', DATE(creation_date)) AS month,
  ROUND(SUM(CASE WHEN REGEXP_CONTAINS(tags, r'(^|\|)python(\||$)') THEN 1 ELSE 0 END) / COUNT(*), 4) AS proportion
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE EXTRACT(YEAR FROM creation_date) = 2022
GROUP BY month
ORDER BY month