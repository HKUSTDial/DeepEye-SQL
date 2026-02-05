WITH qualified_questions AS (
  SELECT 
    q.id,
    q.body,
    LENGTH(q.body) AS body_length,
    q.owner_user_id,
    q.accepted_answer_id,
    q.view_count
  FROM `bigquery-public-data.stackoverflow.posts_questions` q
  WHERE 
    q.accepted_answer_id IS NOT NULL
    OR (
      q.accepted_answer_id IS NULL
      AND EXISTS (
        SELECT 1
        FROM `bigquery-public-data.stackoverflow.posts_answers` a
        WHERE a.parent_id = q.id
          AND q.view_count > 0
          AND CAST(a.score AS FLOAT64) / CAST(q.view_count AS FLOAT64) > 0.01
      )
    )
)
SELECT 
  qq.id AS question_id,
  qq.body,
  qq.body_length,
  u.reputation,
  (u.up_votes - u.down_votes) AS net_votes,
  COUNT(b.id) AS total_badges
FROM qualified_questions qq
INNER JOIN `bigquery-public-data.stackoverflow.users` u
  ON qq.owner_user_id = u.id
LEFT JOIN `bigquery-public-data.stackoverflow.badges` b
  ON u.id = b.user_id
GROUP BY 
  qq.id,
  qq.body,
  qq.body_length,
  u.reputation,
  u.up_votes,
  u.down_votes
ORDER BY qq.body_length DESC
LIMIT 10