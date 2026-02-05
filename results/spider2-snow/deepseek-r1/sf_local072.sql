WITH jan_2022_data AS (
  SELECT
    "country_code_2",
    TO_DATE("insert_date") AS insert_dt
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."CITIES"
  WHERE "insert_date" LIKE '2022-01-%'
),
country_day_counts AS (
  SELECT
    "country_code_2",
    COUNT(DISTINCT insert_dt) AS num_days
  FROM jan_2022_data
  GROUP BY "country_code_2"
),
target_country AS (
  SELECT
    cd."country_code_2",
    cc."country_name"
  FROM country_day_counts cd
  JOIN "CITY_LEGISLATION"."CITY_LEGISLATION"."CITIES_COUNTRIES" cc
    ON cd."country_code_2" = cc."country_code_2"
  WHERE cd.num_days = 9
  LIMIT 1
),
target_country_dates AS (
  SELECT
    DISTINCT TO_DATE("insert_date") AS insert_dt
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."CITIES"
  WHERE "country_code_2" = (SELECT "country_code_2" FROM target_country)
    AND "insert_date" LIKE '2022-01-%'
),
consecutive_groups AS (
  SELECT
    insert_dt,
    DATEADD(day, -ROW_NUMBER() OVER (ORDER BY insert_dt), insert_dt) AS grp
  FROM target_country_dates
),
group_counts AS (
  SELECT
    grp,
    MIN(insert_dt) AS start_date,
    MAX(insert_dt) AS end_date,
    COUNT(*) AS consecutive_days
  FROM consecutive_groups
  GROUP BY grp
),
longest_streak AS (
  SELECT
    grp,
    start_date,
    end_date,
    consecutive_days
  FROM group_counts
  ORDER BY consecutive_days DESC
  LIMIT 1
),
entries_in_streak AS (
  SELECT
    c."capital"
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."CITIES" c
  WHERE c."country_code_2" = (SELECT "country_code_2" FROM target_country)
    AND TO_DATE(c."insert_date") BETWEEN (SELECT start_date FROM longest_streak) AND (SELECT end_date FROM longest_streak)
),
proportion_calc AS (
  SELECT
    COUNT(*) AS total_entries,
    SUM(CASE WHEN "capital" = 1 THEN 1 ELSE 0 END) AS capital_entries,
    capital_entries * 1.0 / total_entries AS proportion
  FROM entries_in_streak
)
SELECT
  tc."country_code_2",
  tc."country_name",
  ls.start_date AS streak_start,
  ls.end_date AS streak_end,
  ls.consecutive_days AS streak_length,
  pc.total_entries,
  pc.capital_entries,
  pc.proportion
FROM target_country tc
CROSS JOIN longest_streak ls
CROSS JOIN proportion_calc pc