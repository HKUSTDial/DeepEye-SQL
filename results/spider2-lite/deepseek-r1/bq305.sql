WITH user_question_associations AS (
  -- Users who own questions
  SELECT owner_user_id as user_id, id as question_id
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE owner_user_id IS NOT NULL
  
  UNION DISTINCT
  
  -- Users with accepted answers
  SELECT a.owner_user_id as user_id, q.id as question_id
  FROM `bigquery-public-data.stackoverflow.posts_questions` q
  JOIN `bigquery-public-data.stackoverflow.posts_answers` a 
    ON q.accepted_answer_id = a.id
  WHERE a.owner_user_id IS NOT NULL
  
  UNION DISTINCT
  
  -- Users with answers scoring > 5
  SELECT owner_user_id as user_id, parent_id as question_id
  FROM `bigquery-public-data.stackoverflow.posts_answers`
  WHERE score > 5 AND owner_user_id IS NOT NULL
  
  UNION DISTINCT
  
  -- Users with answers scoring > 20% of total answer scores (and > 0)
  SELECT a.owner_user_id as user_id, a.parent_id as question_id
  FROM `bigquery-public-data.stackoverflow.posts_answers` a
  JOIN (
    SELECT parent_id, SUM(score) as total_score
    FROM `bigquery-public-data.stackoverflow.posts_answers`
    GROUP BY parent_id
  ) ts ON a.parent_id = ts.parent_id
  WHERE a.score > 0.2 * ts.total_score AND a.score > 0
  AND a.owner_user_id IS NOT NULL
  
  UNION DISTINCT
  
  -- Users with answers among top 3 highest-scoring for a question
  SELECT owner_user_id as user_id, parent_id as question_id
  FROM (
    SELECT parent_id, owner_user_id,
           ROW_NUMBER() OVER (PARTITION BY parent_id ORDER BY score DESC) as rank
    FROM `bigquery-public-data.stackoverflow.posts_answers`
    WHERE owner_user_id IS NOT NULL
  ) ranked
  WHERE rank <= 3
)
SELECT u.id as user_id, u.display_name, SUM(q.view_count) as combined_view_count
FROM user_question_associations assoc
JOIN `bigquery-public-data.stackoverflow.posts_questions` q 
  ON assoc.question_id = q.id
JOIN `bigquery-public-data.stackoverflow.users` u 
  ON assoc.user_id = u.id
GROUP BY u.id, u.display_name
ORDER BY combined_view_count DESC
LIMIT 10