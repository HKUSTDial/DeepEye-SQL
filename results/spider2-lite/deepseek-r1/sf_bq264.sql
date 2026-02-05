WITH filtered_users AS (
  SELECT "id", "age"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS"
  WHERE DATE(TO_TIMESTAMP("created_at" / 1000000)) BETWEEN DATE '2019-01-01' AND DATE '2022-04-30'
),
age_extremes AS (
  SELECT MAX("age") AS max_age, MIN("age") AS min_age
  FROM filtered_users
),
counts AS (
  SELECT 
    COUNT(CASE WHEN "age" = (SELECT max_age FROM age_extremes) THEN 1 END) AS count_oldest,
    COUNT(CASE WHEN "age" = (SELECT min_age FROM age_extremes) THEN 1 END) AS count_youngest
  FROM filtered_users
)
SELECT (count_oldest - count_youngest) AS difference
FROM counts