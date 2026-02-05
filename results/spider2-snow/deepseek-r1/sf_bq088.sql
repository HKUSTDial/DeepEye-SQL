WITH period_2019 AS (
  SELECT 
    AVG(TRY_CAST("symptom_anxiety" AS FLOAT)) AS avg_anxiety_2019,
    AVG(TRY_CAST("symptom_depression" AS FLOAT)) AS avg_depression_2019
  FROM "COVID19_SYMPTOM_SEARCH"."COVID19_SYMPTOM_SEARCH"."SYMPTOM_SEARCH_COUNTRY_WEEKLY"
  WHERE "country_region" = 'United States'
    AND TO_DATE("date", 'YYYY-MM-DD') >= TO_DATE('2019-01-01', 'YYYY-MM-DD')
    AND TO_DATE("date", 'YYYY-MM-DD') < TO_DATE('2020-01-01', 'YYYY-MM-DD')
),
period_2020 AS (
  SELECT 
    AVG(TRY_CAST("symptom_anxiety" AS FLOAT)) AS avg_anxiety_2020,
    AVG(TRY_CAST("symptom_depression" AS FLOAT)) AS avg_depression_2020
  FROM "COVID19_SYMPTOM_SEARCH"."COVID19_SYMPTOM_SEARCH"."SYMPTOM_SEARCH_COUNTRY_WEEKLY"
  WHERE "country_region" = 'United States'
    AND TO_DATE("date", 'YYYY-MM-DD') >= TO_DATE('2020-01-01', 'YYYY-MM-DD')
    AND TO_DATE("date", 'YYYY-MM-DD') < TO_DATE('2021-01-01', 'YYYY-MM-DD')
)
SELECT 
  avg_anxiety_2019,
  avg_depression_2019,
  avg_anxiety_2020,
  avg_depression_2020,
  ((avg_anxiety_2020 - avg_anxiety_2019) / avg_anxiety_2019) * 100 AS pct_increase_anxiety,
  ((avg_depression_2020 - avg_depression_2019) / avg_depression_2019) * 100 AS pct_increase_depression
FROM period_2019 CROSS JOIN period_2020