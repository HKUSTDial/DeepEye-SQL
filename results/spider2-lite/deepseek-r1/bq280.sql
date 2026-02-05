SELECT u.display_name
FROM `bigquery-public-data.stackoverflow.users` u
JOIN `bigquery-public-data.stackoverflow.posts_answers` a ON u.id = a.owner_user_id
WHERE u.reputation > 10
GROUP BY u.id, u.display_name
ORDER BY COUNT(*) DESC
LIMIT 1