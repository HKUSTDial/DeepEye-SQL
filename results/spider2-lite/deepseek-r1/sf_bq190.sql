WITH filtered_users AS (
  SELECT "gender", "age", "id"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS"
  WHERE TO_DATE(TO_TIMESTAMP("created_at" / 1000000)) BETWEEN '2019-01-01' AND '2022-04-30'
    AND "gender" IN ('M', 'F')
),
age_extremes AS (
  SELECT "gender",
         MIN("age") AS min_age,
         MAX("age") AS max_age
  FROM filtered_users
  GROUP BY "gender"
)
SELECT 
  f."gender",
  ae.min_age AS "age",
  'youngest' AS "age_group",
  COUNT(*) AS "user_count"
FROM filtered_users f
JOIN age_extremes ae ON f."gender" = ae."gender"
WHERE f."age" = ae.min_age
GROUP BY f."gender", ae.min_age
UNION ALL
SELECT 
  f."gender",
  ae.max_age AS "age",
  'oldest' AS "age_group",
  COUNT(*) AS "user_count"
FROM filtered_users f
JOIN age_extremes ae ON f."gender" = ae."gender"
WHERE f."age" = ae.max_age
GROUP BY f."gender", ae.max_age
ORDER BY "gender", "age_group"