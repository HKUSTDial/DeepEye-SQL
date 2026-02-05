WITH countries_with_many_customers AS (
    SELECT "Country"
    FROM "CHINOOK"."CHINOOK"."CUSTOMERS"
    GROUP BY "Country"
    HAVING COUNT(*) > 4
),
total_sales_per_country AS (
    SELECT c."Country", SUM(i."Total") AS total_sales
    FROM countries_with_many_customers cwc
    JOIN "CHINOOK"."CHINOOK"."CUSTOMERS" c ON cwc."Country" = c."Country"
    JOIN "CHINOOK"."CHINOOK"."INVOICES" i ON c."CustomerId" = i."CustomerId"
    GROUP BY c."Country"
)
SELECT MEDIAN(total_sales) AS median_total_sales
FROM total_sales_per_country