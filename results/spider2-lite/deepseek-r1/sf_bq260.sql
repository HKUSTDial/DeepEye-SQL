WITH filtered_users AS (
  SELECT "gender", "age"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS"
  WHERE DATE(TO_TIMESTAMP("created_at" / 1000000)) BETWEEN DATE '2019-01-01' AND DATE '2022-04-30'
),
age_extremes AS (
  SELECT "gender", MIN("age") AS min_age, MAX("age") AS max_age
  FROM filtered_users
  GROUP BY "gender"
),
age_counts AS (
  SELECT "gender", "age", COUNT(*) AS user_count
  FROM filtered_users
  GROUP BY "gender", "age"
)
SELECT ac."gender", 'youngest' AS age_type, ac."age", ac.user_count
FROM age_counts ac
INNER JOIN age_extremes ae ON ac."gender" = ae."gender" AND ac."age" = ae.min_age
UNION ALL
SELECT ac."gender", 'oldest' AS age_type, ac."age", ac.user_count
FROM age_counts ac
INNER JOIN age_extremes ae ON ac."gender" = ae."gender" AND ac."age" = ae.max_age
ORDER BY "gender", age_type