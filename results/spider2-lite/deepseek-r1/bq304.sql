WITH specified_tags AS (
  SELECT tag
  FROM UNNEST(['android-layout', 'android-activity', 'android-intent', 'android-edittext', 'android-fragments', 'android-recyclerview', 'listview', 'android-actionbar', 'google-maps', 'android-asynctask']) AS tag
),
filtered_questions AS (
  SELECT 
    id,
    title,
    body,
    tags,
    view_count
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE 
    (CONTAINS_SUBSTR(LOWER(title), 'how') OR CONTAINS_SUBSTR(LOWER(body), 'how'))
    AND NOT (
      CONTAINS_SUBSTR(LOWER(title), 'fail') OR CONTAINS_SUBSTR(LOWER(body), 'fail') OR
      CONTAINS_SUBSTR(LOWER(title), 'problem') OR CONTAINS_SUBSTR(LOWER(body), 'problem') OR
      CONTAINS_SUBSTR(LOWER(title), 'error') OR CONTAINS_SUBSTR(LOWER(body), 'error') OR
      CONTAINS_SUBSTR(LOWER(title), 'wrong') OR CONTAINS_SUBSTR(LOWER(body), 'wrong') OR
      CONTAINS_SUBSTR(LOWER(title), 'fix') OR CONTAINS_SUBSTR(LOWER(body), 'fix') OR
      CONTAINS_SUBSTR(LOWER(title), 'bug') OR CONTAINS_SUBSTR(LOWER(body), 'bug') OR
      CONTAINS_SUBSTR(LOWER(title), 'issue') OR CONTAINS_SUBSTR(LOWER(body), 'issue') OR
      CONTAINS_SUBSTR(LOWER(title), 'solve') OR CONTAINS_SUBSTR(LOWER(body), 'solve') OR
      CONTAINS_SUBSTR(LOWER(title), 'trouble') OR CONTAINS_SUBSTR(LOWER(body), 'trouble')
    )
),
questions_with_tags AS (
  SELECT 
    fq.*,
    st.tag
  FROM filtered_questions fq
  CROSS JOIN specified_tags st
  WHERE 
    REGEXP_CONTAINS(fq.tags, CONCAT(r'^(.*\|)?', st.tag, r'(\|.*)?$'))
),
tag_counts AS (
  SELECT 
    tag,
    COUNT(DISTINCT id) AS question_count
  FROM questions_with_tags
  GROUP BY tag
  HAVING COUNT(DISTINCT id) >= 50
),
ranked_questions AS (
  SELECT 
    qwt.*,
    ROW_NUMBER() OVER (PARTITION BY qwt.tag ORDER BY qwt.view_count DESC) AS rank
  FROM questions_with_tags qwt
  INNER JOIN tag_counts tc ON qwt.tag = tc.tag
)
SELECT 
  tag,
  id,
  title,
  body,
  tags,
  view_count
FROM ranked_questions
WHERE rank <= 50
ORDER BY tag, rank