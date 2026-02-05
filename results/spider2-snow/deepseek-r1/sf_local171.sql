WITH male_la_first_term AS (
  SELECT l."id_bioguide", MIN(TO_DATE(t."term_start", 'YYYY-MM-DD')) AS first_term_start
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."LEGISLATORS" l
  JOIN "CITY_LEGISLATION"."CITY_LEGISLATION"."LEGISLATORS_TERMS" t ON l."id_bioguide" = t."id_bioguide"
  WHERE l."gender" = 'M' AND t."state" = 'LA'
  GROUP BY l."id_bioguide"
),
year_offsets AS (
  SELECT ROW_NUMBER() OVER (ORDER BY NULL) + 30 AS year_offset
  FROM TABLE(GENERATOR(ROWCOUNT => 19))
),
candidate_dates AS (
  SELECT m."id_bioguide", m.first_term_start, DATE_FROM_PARTS(YEAR(m.first_term_start) + y.year_offset, 12, 31) AS dec_31_date, y.year_offset
  FROM male_la_first_term m
  CROSS JOIN year_offsets y
  WHERE y.year_offset > 30 AND y.year_offset < 50
),
active_legislators AS (
  SELECT DISTINCT c."id_bioguide", c.year_offset
  FROM candidate_dates c
  JOIN "CITY_LEGISLATION"."CITY_LEGISLATION"."LEGISLATORS_TERMS" t 
    ON c."id_bioguide" = t."id_bioguide"
    AND t."state" = 'LA'
    AND TO_DATE(t."term_start", 'YYYY-MM-DD') <= c.dec_31_date
    AND TO_DATE(t."term_end", 'YYYY-MM-DD') >= c.dec_31_date
)
SELECT year_offset AS years_since_first_term, COUNT(DISTINCT "id_bioguide") AS distinct_legislators
FROM active_legislators
GROUP BY year_offset
ORDER BY year_offset