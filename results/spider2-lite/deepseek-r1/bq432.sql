WITH cleaned_events AS (
  SELECT 
    report_number,
    REPLACE(REGEXP_REPLACE(products_brand_name, r'(\d), (\d)', '\\1,\\2'), ', ', ' -- ') AS products_brand_name_cleaned,
    date_started,
    date_created,
    consumer_age_unit,
    SPLIT(outcomes, ', ') AS outcomes_array,
    REPLACE(products_industry_code, ', ', ' -- ') AS products_industry_code_cleaned,
    consumer_gender,
    SPLIT(reactions, ', ') AS reactions_array,
    REPLACE(products_industry_name, ', ', ' -- ') AS products_industry_name_cleaned,
    REPLACE(products_role, ', ', ' -- ') AS products_role_cleaned,
    consumer_age
  FROM `bigquery-public-data.fda_food.food_events`
  WHERE date_created BETWEEN DATE '2015-01-01' AND DATE '2015-01-31'
    AND date_started BETWEEN DATE '2015-01-01' AND DATE '2015-01-31'
)
SELECT 
  report_number,
  products_brand_name_cleaned AS products_brand_name,
  date_started,
  date_created,
  consumer_age_unit,
  outcomes_array AS outcomes,
  products_industry_code_cleaned AS products_industry_code,
  consumer_gender,
  reactions_array AS reactions,
  products_industry_name_cleaned AS products_industry_name,
  products_role_cleaned AS products_role,
  consumer_age,
  ARRAY_LENGTH(SPLIT(products_industry_code_cleaned, ' -- ')) AS industry_code_length,
  ARRAY_LENGTH(SPLIT(products_brand_name_cleaned, ' -- ')) AS brand_name_length
FROM cleaned_events