SELECT 
  answers.id AS answer_id,
  answerer_users.reputation AS answerer_reputation,
  answers.score AS answer_score,
  answers.comment_count AS answer_comment_count,
  questions.tags AS question_tags,
  questions.score AS question_score,
  questions.answer_count AS question_answer_count,
  asker_users.reputation AS asker_reputation,
  questions.view_count AS question_view_count,
  questions.comment_count AS question_comment_count
FROM `bigquery-public-data.stackoverflow.posts_questions` AS questions
INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS answers
  ON questions.accepted_answer_id = answers.id
INNER JOIN `bigquery-public-data.stackoverflow.users` AS asker_users
  ON questions.owner_user_id = asker_users.id
INNER JOIN `bigquery-public-data.stackoverflow.users` AS answerer_users
  ON answers.owner_user_id = answerer_users.id
WHERE EXTRACT(YEAR FROM questions.creation_date) = 2016
  AND EXTRACT(MONTH FROM questions.creation_date) = 1
  AND EXTRACT(YEAR FROM answers.creation_date) = 2016
  AND EXTRACT(MONTH FROM answers.creation_date) = 1
  AND CONTAINS_SUBSTR(questions.tags, 'javascript')
  AND (CONTAINS_SUBSTR(questions.tags, 'xss')
    OR CONTAINS_SUBSTR(questions.tags, 'cross-site')
    OR CONTAINS_SUBSTR(questions.tags, 'exploit')
    OR CONTAINS_SUBSTR(questions.tags, 'cybersecurity'))