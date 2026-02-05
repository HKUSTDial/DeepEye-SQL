WITH monthly_totals AS (
    SELECT 
        i."CustomerID",
        EXTRACT(MONTH FROM TO_DATE(i."InvoiceDate")) AS month,
        SUM(l."ExtendedPrice") AS monthly_total
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICES" i
    JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICELINES" l ON i."InvoiceID" = l."InvoiceID"
    WHERE EXTRACT(YEAR FROM TO_DATE(i."InvoiceDate")) = 2014
    GROUP BY i."CustomerID", EXTRACT(MONTH FROM TO_DATE(i."InvoiceDate"))
),
customer_averages AS (
    SELECT 
        "CustomerID",
        AVG(monthly_total) AS avg_monthly_spending
    FROM monthly_totals
    GROUP BY "CustomerID"
)
SELECT MEDIAN(avg_monthly_spending) AS median_avg_monthly_spending
FROM customer_averages