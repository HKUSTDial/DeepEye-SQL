SELECT 
  `category`,
  COUNT(*) AS total_articles,
  SUM(CASE WHEN CONTAINS_SUBSTR(`body`, 'education') OR CONTAINS_SUBSTR(`title`, 'education') THEN 1 ELSE 0 END) AS education_articles,
  (SUM(CASE WHEN CONTAINS_SUBSTR(`body`, 'education') OR CONTAINS_SUBSTR(`title`, 'education') THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS education_percentage
FROM `bigquery-public-data.bbc_news.fulltext`
GROUP BY `category`
ORDER BY `category`