WITH filtered_users AS (
  SELECT 
    "id",
    "reputation",
    TO_DATE(TO_TIMESTAMP("creation_date" / 1000000)) AS join_date,
    FLOOR(DATEDIFF('month', TO_DATE(TO_TIMESTAMP("creation_date" / 1000000)), TO_DATE('2021-10-01', 'YYYY-MM-DD')) / 12) AS tenure_years
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."USERS"
  WHERE join_date <= TO_DATE('2021-10-01', 'YYYY-MM-DD')
),
user_badges AS (
  SELECT 
    "user_id",
    COUNT(*) AS badge_count
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."BADGES"
  GROUP BY "user_id"
)
SELECT 
  f.tenure_years,
  AVG(f."reputation") AS avg_reputation,
  AVG(COALESCE(u.badge_count, 0)) AS avg_badges_per_user
FROM filtered_users f
LEFT JOIN user_badges u ON f."id" = u."user_id"
GROUP BY f.tenure_years
ORDER BY f.tenure_years