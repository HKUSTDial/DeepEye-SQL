WITH period_data AS (
  SELECT 
    PARSE_DATE('%Y-%m-%d', date) AS date_d,
    CAST(symptom_anxiety AS FLOAT64) AS anxiety,
    CAST(symptom_depression AS FLOAT64) AS depression
  FROM `bigquery-public-data.covid19_symptom_search.symptom_search_country_weekly`
  WHERE country_region = 'United States'
    AND PARSE_DATE('%Y-%m-%d', date) >= '2019-01-01'
    AND PARSE_DATE('%Y-%m-%d', date) < '2021-01-01'
),
averages AS (
  SELECT
    AVG(CASE WHEN date_d >= '2019-01-01' AND date_d < '2020-01-01' THEN anxiety END) AS avg_anxiety_2019,
    AVG(CASE WHEN date_d >= '2019-01-01' AND date_d < '2020-01-01' THEN depression END) AS avg_depression_2019,
    AVG(CASE WHEN date_d >= '2020-01-01' AND date_d < '2021-01-01' THEN anxiety END) AS avg_anxiety_2020,
    AVG(CASE WHEN date_d >= '2020-01-01' AND date_d < '2021-01-01' THEN depression END) AS avg_depression_2020
  FROM period_data
)
SELECT
  avg_anxiety_2019,
  avg_depression_2019,
  avg_anxiety_2020,
  avg_depression_2020,
  (avg_anxiety_2020 - avg_anxiety_2019) / avg_anxiety_2019 * 100 AS pct_increase_anxiety,
  (avg_depression_2020 - avg_depression_2019) / avg_depression_2019 * 100 AS pct_increase_depression
FROM averages