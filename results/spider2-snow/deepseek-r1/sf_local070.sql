WITH distinct_dates AS (
  SELECT DISTINCT
    TO_DATE("insert_date") AS "date"
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."CITIES"
  WHERE "country_code_2" = 'cn'
    AND "insert_date" >= '2021-07-01'
    AND "insert_date" <= '2021-07-31'
),
numbered_dates AS (
  SELECT
    "date",
    ROW_NUMBER() OVER (ORDER BY "date") AS rn
  FROM distinct_dates
),
grouped_dates AS (
  SELECT
    "date",
    DATEADD(day, -rn, "date") AS group_start
  FROM numbered_dates
),
streaks AS (
  SELECT
    MIN("date") AS start_date,
    MAX("date") AS end_date,
    COUNT(*) AS streak_length
  FROM grouped_dates
  GROUP BY group_start
),
min_max AS (
  SELECT
    MIN(streak_length) AS min_len,
    MAX(streak_length) AS max_len
  FROM streaks
),
selected_streaks AS (
  SELECT s.*
  FROM streaks s
  CROSS JOIN min_max m
  WHERE s.streak_length = m.min_len OR s.streak_length = m.max_len
),
streak_dates AS (
  SELECT
    ss.start_date,
    ss.end_date,
    DATEADD(day, n.n, ss.start_date) AS "date"
  FROM selected_streaks ss
  CROSS JOIN (
    SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS n
    FROM TABLE(GENERATOR(ROWCOUNT => 31))
  ) n
  WHERE n.n <= DATEDIFF(day, ss.start_date, ss.end_date)
),
city_per_date AS (
  SELECT
    TO_DATE("insert_date") AS "date",
    "city_name",
    ROW_NUMBER() OVER (PARTITION BY TO_DATE("insert_date") ORDER BY "city_id") AS rn
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."CITIES"
  WHERE "country_code_2" = 'cn'
    AND "insert_date" >= '2021-07-01'
    AND "insert_date" <= '2021-07-31'
),
distinct_city_per_date AS (
  SELECT "date", "city_name"
  FROM city_per_date
  WHERE rn = 1
)
SELECT
  sd."date",
  INITCAP(dc."city_name") AS "city_name"
FROM streak_dates sd
JOIN distinct_city_per_date dc ON sd."date" = dc."date"
ORDER BY sd."date"