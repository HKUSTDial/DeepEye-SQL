WITH state_daily_new AS (
  SELECT 
    state_name,
    date,
    confirmed_cases - LAG(confirmed_cases, 1) OVER (PARTITION BY state_name ORDER BY date) AS new_cases
  FROM `bigquery-public-data.covid19_nyt.us_states`
  WHERE date BETWEEN '2020-03-01' AND '2020-05-31'
),
state_daily_new_filtered AS (
  SELECT * FROM state_daily_new WHERE new_cases IS NOT NULL
),
daily_top_states AS (
  SELECT 
    date,
    state_name,
    new_cases,
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY new_cases DESC, state_name ASC) AS rank
  FROM state_daily_new_filtered
),
state_appearances AS (
  SELECT state_name, COUNT(*) AS appearance_count
  FROM daily_top_states
  WHERE rank <= 5
  GROUP BY state_name
),
state_ranking AS (
  SELECT state_name, appearance_count,
    ROW_NUMBER() OVER (ORDER BY appearance_count DESC, state_name ASC) AS rank
  FROM state_appearances
),
fourth_state AS (
  SELECT state_name FROM state_ranking WHERE rank = 4
),
county_daily_new AS (
  SELECT 
    c.state_name,
    c.county,
    c.date,
    c.confirmed_cases - LAG(c.confirmed_cases, 1) OVER (PARTITION BY c.state_name, c.county ORDER BY c.date) AS new_cases
  FROM `bigquery-public-data.covid19_nyt.us_counties` c
  INNER JOIN fourth_state f ON c.state_name = f.state_name
  WHERE c.date BETWEEN '2020-03-01' AND '2020-05-31'
),
county_daily_new_filtered AS (
  SELECT * FROM county_daily_new WHERE new_cases IS NOT NULL
),
daily_top_counties AS (
  SELECT 
    date,
    county,
    new_cases,
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY new_cases DESC, county ASC) AS rank
  FROM county_daily_new_filtered
),
county_appearances AS (
  SELECT county, COUNT(*) AS appearance_count
  FROM daily_top_counties
  WHERE rank <= 5
  GROUP BY county
),
county_ranking AS (
  SELECT county, appearance_count,
    ROW_NUMBER() OVER (ORDER BY appearance_count DESC, county ASC) AS rank
  FROM county_appearances
)
SELECT 'state' AS category, state_name AS name, appearance_count, rank FROM state_ranking
UNION ALL
SELECT 'county' AS category, county AS name, appearance_count, rank FROM county_ranking WHERE rank <= 5
ORDER BY category, rank