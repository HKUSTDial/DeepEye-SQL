WITH filtered_users AS (
  SELECT "id", "age"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS"
  WHERE TO_TIMESTAMP("created_at" / 1000000) >= '2019-01-01 00:00:00'
    AND TO_TIMESTAMP("created_at" / 1000000) < '2022-05-01 00:00:00'
),
age_extremes AS (
  SELECT MAX("age") AS max_age, MIN("age") AS min_age
  FROM filtered_users
),
counts AS (
  SELECT
    (SELECT COUNT(*) FROM filtered_users WHERE "age" = (SELECT max_age FROM age_extremes)) AS count_max_age,
    (SELECT COUNT(*) FROM filtered_users WHERE "age" = (SELECT min_age FROM age_extremes)) AS count_min_age
)
SELECT (count_max_age - count_min_age) AS difference
FROM counts