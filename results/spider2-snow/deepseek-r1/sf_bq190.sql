WITH filtered_users AS (
  SELECT "gender", "age"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS"
  WHERE "gender" IN ('M', 'F')
    AND TO_DATE(TO_TIMESTAMP_NTZ("created_at" / 1000000)) BETWEEN '2019-01-01' AND '2022-04-30'
),
gender_age_bounds AS (
  SELECT "gender", MIN("age") AS "min_age", MAX("age") AS "max_age"
  FROM filtered_users
  GROUP BY "gender"
)
SELECT f."gender", 'youngest' AS "extreme_type", f."age", COUNT(*) AS "user_count"
FROM filtered_users f
INNER JOIN gender_age_bounds g ON f."gender" = g."gender" AND f."age" = g."min_age"
GROUP BY f."gender", f."age"
UNION ALL
SELECT f."gender", 'oldest' AS "extreme_type", f."age", COUNT(*) AS "user_count"
FROM filtered_users f
INNER JOIN gender_age_bounds g ON f."gender" = g."gender" AND f."age" = g."max_age"
GROUP BY f."gender", f."age"
ORDER BY "gender", "extreme_type"