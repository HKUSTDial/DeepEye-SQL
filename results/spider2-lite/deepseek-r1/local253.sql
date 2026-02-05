WITH cleaned_all AS (
    SELECT 
        CompanyName,
        Location,
        CAST(REPLACE(REPLACE(REPLACE(Salary, '₹', ''), ',', ''), '/yr', '') AS INTEGER) AS salary_numeric
    FROM SalaryDataset
),
filtered AS (
    SELECT * FROM cleaned_all WHERE Location IN ('Mumbai', 'Pune', 'New Delhi', 'Hyderabad')
),
avg_per_company_location AS (
    SELECT 
        Location,
        CompanyName,
        AVG(salary_numeric) AS avg_salary_state
    FROM filtered
    GROUP BY Location, CompanyName
),
overall_national_avg AS (
    SELECT AVG(salary_numeric) AS avg_salary_country
    FROM cleaned_all
),
ranked AS (
    SELECT 
        a.Location,
        a.CompanyName,
        a.avg_salary_state,
        o.avg_salary_country,
        ROW_NUMBER() OVER (PARTITION BY a.Location ORDER BY a.avg_salary_state DESC) AS rn
    FROM avg_per_company_location a
    CROSS JOIN overall_national_avg o
)
SELECT 
    Location AS "Location",
    CompanyName AS "Company Name",
    avg_salary_state AS "Average Salary in State",
    avg_salary_country AS "Average Salary in Country"
FROM ranked
WHERE rn <= 5
ORDER BY Location, rn;