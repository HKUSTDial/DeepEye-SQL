WITH filtered_answers AS (
  SELECT id AS answer_id, parent_id AS question_id
  FROM `bigquery-public-data.stackoverflow.posts_answers`
  WHERE owner_user_id = 1908967
    AND creation_date < TIMESTAMP('2018-06-07')
),
answer_tags AS (
  SELECT DISTINCT fa.answer_id, tag
  FROM filtered_answers fa
  JOIN `bigquery-public-data.stackoverflow.posts_questions` q
    ON fa.question_id = q.id
  CROSS JOIN UNNEST(SPLIT(q.tags, '|')) AS tag
),
answer_votes AS (
  SELECT 
    fa.answer_id,
    COUNTIF(v.vote_type_id = 1) AS accepted_count,
    COUNTIF(v.vote_type_id = 2) AS upvote_count
  FROM filtered_answers fa
  LEFT JOIN `bigquery-public-data.stackoverflow.votes` v
    ON fa.answer_id = v.post_id
    AND v.vote_type_id IN (1,2)
  GROUP BY fa.answer_id
)
SELECT 
  tag,
  SUM(10 * upvote_count + 15 * accepted_count) AS total_score
FROM answer_tags ans_tags
JOIN answer_votes av ON ans_tags.answer_id = av.answer_id
GROUP BY tag
ORDER BY total_score DESC
LIMIT 10