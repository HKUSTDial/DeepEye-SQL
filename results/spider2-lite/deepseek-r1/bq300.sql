SELECT MAX(answer_count) AS highest_answer_count
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE post_type_id = 1
  AND REGEXP_CONTAINS(tags, r'(^|\|)python-2')
  AND NOT REGEXP_CONTAINS(tags, r'(^|\|)python-3')