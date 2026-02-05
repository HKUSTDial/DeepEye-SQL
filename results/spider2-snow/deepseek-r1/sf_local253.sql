WITH cleaned_salaries AS (
  SELECT 
    "Location",
    "CompanyName",
    TRY_TO_NUMBER(REGEXP_REPLACE("Salary", '[^0-9]', '')) AS cleaned_salary
  FROM "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."SALARYDATASET"
),
national_avg AS (
  SELECT 
    AVG(cleaned_salary) AS avg_national_salary
  FROM cleaned_salaries
  WHERE cleaned_salary IS NOT NULL
),
city_company_avg AS (
  SELECT 
    "Location",
    "CompanyName",
    AVG(cleaned_salary) AS avg_city_salary
  FROM cleaned_salaries
  WHERE "Location" IN ('Mumbai', 'Pune', 'New Delhi', 'Hyderabad')
    AND cleaned_salary IS NOT NULL
  GROUP BY "Location", "CompanyName"
),
ranked AS (
  SELECT 
    "Location",
    "CompanyName",
    avg_city_salary,
    ROW_NUMBER() OVER (PARTITION BY "Location" ORDER BY avg_city_salary DESC) AS rank
  FROM city_company_avg
)
SELECT 
  r."Location",
  r."CompanyName" AS "Company Name",
  r.avg_city_salary AS "Average Salary in State",
  na.avg_national_salary AS "Average Salary in Country"
FROM ranked r
CROSS JOIN national_avg na
WHERE r.rank <= 5
ORDER BY r."Location", r.rank