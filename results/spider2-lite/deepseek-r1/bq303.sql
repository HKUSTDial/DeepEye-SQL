SELECT c.user_id, q.tags
FROM `bigquery-public-data.stackoverflow.comments` c
JOIN `bigquery-public-data.stackoverflow.posts_questions` q ON c.post_id = q.id
WHERE c.creation_date BETWEEN TIMESTAMP('2019-07-01') AND TIMESTAMP('2019-12-31 23:59:59.999999')
  AND c.user_id BETWEEN 16712208 AND 18712208
UNION ALL
SELECT c.user_id, q.tags
FROM `bigquery-public-data.stackoverflow.comments` c
JOIN `bigquery-public-data.stackoverflow.posts_answers` a ON c.post_id = a.id
JOIN `bigquery-public-data.stackoverflow.posts_questions` q ON a.parent_id = q.id
WHERE c.creation_date BETWEEN TIMESTAMP('2019-07-01') AND TIMESTAMP('2019-12-31 23:59:59.999999')
  AND c.user_id BETWEEN 16712208 AND 18712208
UNION ALL
SELECT a.owner_user_id AS user_id, q.tags
FROM `bigquery-public-data.stackoverflow.posts_answers` a
JOIN `bigquery-public-data.stackoverflow.posts_questions` q ON a.parent_id = q.id
WHERE a.creation_date BETWEEN TIMESTAMP('2019-07-01') AND TIMESTAMP('2019-12-31 23:59:59.999999')
  AND a.owner_user_id BETWEEN 16712208 AND 18712208
UNION ALL
SELECT q.owner_user_id AS user_id, q.tags
FROM `bigquery-public-data.stackoverflow.posts_questions` q
WHERE q.creation_date BETWEEN TIMESTAMP('2019-07-01') AND TIMESTAMP('2019-12-31 23:59:59.999999')
  AND q.owner_user_id BETWEEN 16712208 AND 18712208