WITH filtered_users AS (
  SELECT 
    "id",
    "gender",
    "age",
    "created_at"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS"
  WHERE DATE(TO_TIMESTAMP("created_at" / 1000000)) BETWEEN DATE '2019-01-01' AND DATE '2022-04-30'
),
extremes AS (
  SELECT 
    "gender",
    MIN("age") AS "youngest_age",
    MAX("age") AS "oldest_age"
  FROM filtered_users
  GROUP BY "gender"
)
SELECT 
  fu."gender",
  'youngest' AS "age_type",
  ex."youngest_age" AS "age",
  COUNT(*) AS "user_count"
FROM filtered_users fu
INNER JOIN extremes ex 
  ON fu."gender" = ex."gender" 
  AND fu."age" = ex."youngest_age"
GROUP BY fu."gender", ex."youngest_age"
UNION ALL
SELECT 
  fu."gender",
  'oldest' AS "age_type",
  ex."oldest_age" AS "age",
  COUNT(*) AS "user_count"
FROM filtered_users fu
INNER JOIN extremes ex 
  ON fu."gender" = ex."gender" 
  AND fu."age" = ex."oldest_age"
GROUP BY fu."gender", ex."oldest_age"
ORDER BY "gender", "age_type"