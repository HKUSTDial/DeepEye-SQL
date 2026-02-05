WITH monthly_totals AS (
  SELECT 
    EXTRACT(MONTH FROM TO_TIMESTAMP("creation_date" / 1000000)) as month,
    COUNT(*) as total_questions
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("creation_date" / 1000000)) = 2022
  GROUP BY EXTRACT(MONTH FROM TO_TIMESTAMP("creation_date" / 1000000))
),
monthly_python AS (
  SELECT 
    EXTRACT(MONTH FROM TO_TIMESTAMP("creation_date" / 1000000)) as month,
    COUNT(*) as python_questions
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("creation_date" / 1000000)) = 2022
    AND ("tags" LIKE '%|python|%' OR "tags" LIKE 'python|%' OR "tags" LIKE '%|python' OR "tags" = 'python')
  GROUP BY EXTRACT(MONTH FROM TO_TIMESTAMP("creation_date" / 1000000))
)
SELECT 
  t.month,
  COALESCE(p.python_questions, 0) * 1.0 / t.total_questions as proportion
FROM monthly_totals t
LEFT JOIN monthly_python p ON t.month = p.month
ORDER BY t.month