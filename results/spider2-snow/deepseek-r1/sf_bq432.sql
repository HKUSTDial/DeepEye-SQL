WITH cleaned_data AS (
  SELECT 
    "products_industry_name",
    "date_started",
    "consumer_gender",
    "report_number",
    "reactions",
    "products_brand_name",
    "consumer_age_unit",
    "date_created",
    "outcomes",
    "products_industry_code",
    "products_role",
    "consumer_age",
    -- Split reactions into array
    SPLIT("reactions", ',') AS "reactions_array",
    -- Split outcomes into array  
    SPLIT("outcomes", ',') AS "outcomes_array",
    -- Handle products_brand_name: preserve numeric patterns like "1, 2", then replace other ", " with " -- "
    REPLACE(
      REGEXP_REPLACE("products_brand_name", '([0-9]), ([0-9])', '\\1,--\\2'),
      ', ',
      ' -- '
    ) AS "cleaned_products_brand_name",
    -- Replace ", " with " -- " in products_industry_code
    REPLACE("products_industry_code", ', ', ' -- ') AS "cleaned_products_industry_code",
    -- Replace ", " with " -- " in products_role
    REPLACE("products_role", ', ', ' -- ') AS "cleaned_products_role",
    -- Replace ", " with " -- " in products_industry_name
    REPLACE("products_industry_name", ', ', ' -- ') AS "cleaned_products_industry_name"
  FROM "FDA"."FDA_FOOD"."FOOD_EVENTS"
  WHERE 
    "date_created" BETWEEN '2015-01-01' AND '2015-01-31'
    AND "date_started" BETWEEN '2015-01-01' AND '2015-01-31'
)
SELECT 
  "cleaned_products_industry_name",
  "date_started",
  "consumer_gender",
  "report_number",
  "reactions_array",
  "cleaned_products_brand_name",
  "consumer_age_unit",
  "date_created",
  "outcomes_array",
  "cleaned_products_industry_code",
  "cleaned_products_role",
  "consumer_age",
  -- Calculate industry_code_length as array length after splitting
  ARRAY_SIZE(SPLIT("cleaned_products_industry_code", ' -- ')) AS "industry_code_length",
  -- Calculate brand_name_length as array length after splitting  
  ARRAY_SIZE(SPLIT("cleaned_products_brand_name", ' -- ')) AS "brand_name_length"
FROM cleaned_data