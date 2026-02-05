WITH first_gold AS (
  SELECT 
    "user_id",
    "name",
    "date",
    ROW_NUMBER() OVER (PARTITION BY "user_id" ORDER BY "date") AS rn
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."BADGES"
  WHERE "class" = 1
)
SELECT 
  fg."name" AS badge_name,
  COUNT(*) AS user_count,
  AVG((fg."date" - u."creation_date") / 86400000000.0) AS avg_days
FROM first_gold fg
JOIN "STACKOVERFLOW"."STACKOVERFLOW"."USERS" u ON fg."user_id" = u."id"
WHERE fg.rn = 1
GROUP BY fg."name"
ORDER BY user_count DESC
LIMIT 10