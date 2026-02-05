WITH "year_offsets" AS (
  SELECT ROW_NUMBER() OVER (ORDER BY NULL) - 1 AS "offset"
  FROM TABLE(GENERATOR(ROWCOUNT => 20))
),
"periods" AS (
  SELECT "offset" + 1 AS "period_year"
  FROM "year_offsets"
),
"cohort" AS (
  SELECT "id_bioguide", MIN(TO_DATE("term_start")) AS "first_term_start"
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."LEGISLATORS_TERMS"
  GROUP BY "id_bioguide"
  HAVING MIN(TO_DATE("term_start")) BETWEEN TO_DATE('1917-01-01') AND TO_DATE('1999-12-31')
),
"cohort_periods" AS (
  SELECT 
    c."id_bioguide",
    c."first_term_start",
    p."period_year",
    DATE_FROM_PARTS(YEAR(c."first_term_start") + p."period_year" - 1, 12, 31) AS "retention_date"
  FROM "cohort" c
  CROSS JOIN "periods" p
),
"retention_check" AS (
  SELECT 
    cp."id_bioguide",
    cp."period_year",
    CASE WHEN EXISTS (
      SELECT 1 
      FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."LEGISLATORS_TERMS" lt 
      WHERE lt."id_bioguide" = cp."id_bioguide"
        AND TO_DATE(lt."term_start") <= cp."retention_date"
        AND TO_DATE(lt."term_end") >= cp."retention_date"
    ) THEN 1 ELSE 0 END AS "retained"
  FROM "cohort_periods" cp
),
"aggregated" AS (
  SELECT 
    rc."period_year",
    COUNT(DISTINCT rc."id_bioguide") AS "retained_count"
  FROM "retention_check" rc
  WHERE rc."retained" = 1
  GROUP BY rc."period_year"
),
"cohort_size" AS (
  SELECT COUNT(DISTINCT "id_bioguide") AS "total_cohort"
  FROM "cohort"
)
SELECT 
  p."period_year",
  COALESCE(a."retained_count", 0) AS "retained_count",
  cs."total_cohort",
  COALESCE(a."retained_count", 0) * 1.0 / cs."total_cohort" AS "retention_rate"
FROM "periods" p
CROSS JOIN "cohort_size" cs
LEFT JOIN "aggregated" a ON p."period_year" = a."period_year"
ORDER BY p."period_year"