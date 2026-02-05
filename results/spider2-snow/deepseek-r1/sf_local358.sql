WITH "user_age" AS (
  SELECT
    "user_id",
    TO_DATE("birth_date", 'YYYY-MM-DD') AS "birth_date_dt"
  FROM "LOG"."LOG"."MST_USERS"
),
"calculated_age" AS (
  SELECT
    "user_id",
    DATEDIFF(YEAR, "birth_date_dt", CURRENT_DATE()) -
      CASE
        WHEN MONTH(CURRENT_DATE()) < MONTH("birth_date_dt")
          OR (MONTH(CURRENT_DATE()) = MONTH("birth_date_dt") AND DAY(CURRENT_DATE()) < DAY("birth_date_dt"))
        THEN 1
        ELSE 0
      END AS "age"
  FROM "user_age"
),
"age_categories" AS (
  SELECT
    "user_id",
    CASE
      WHEN "age" BETWEEN 20 AND 29 THEN '20s'
      WHEN "age" BETWEEN 30 AND 39 THEN '30s'
      WHEN "age" BETWEEN 40 AND 49 THEN '40s'
      WHEN "age" BETWEEN 50 AND 59 THEN '50s'
      ELSE 'others'
    END AS "age_category"
  FROM "calculated_age"
),
"category_list" AS (
  SELECT '20s' AS "age_category"
  UNION ALL
  SELECT '30s'
  UNION ALL
  SELECT '40s'
  UNION ALL
  SELECT '50s'
  UNION ALL
  SELECT 'others'
)
SELECT
  c."age_category",
  COUNT(a."user_id") AS "user_count"
FROM "category_list" c
LEFT JOIN "age_categories" a ON c."age_category" = a."age_category"
GROUP BY c."age_category"
ORDER BY
  CASE c."age_category"
    WHEN '20s' THEN 1
    WHEN '30s' THEN 2
    WHEN '40s' THEN 3
    WHEN '50s' THEN 4
    WHEN 'others' THEN 5
  END