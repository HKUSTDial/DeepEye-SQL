WITH monthly_totals AS (
  SELECT 
    i."CustomerID",
    DATE_TRUNC('month', CAST(i."InvoiceDate" AS DATE)) as month_start,
    SUM(l."ExtendedPrice") as monthly_total
  FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICES" i
  JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICELINES" l 
    ON i."InvoiceID" = l."InvoiceID"
  WHERE EXTRACT(YEAR FROM CAST(i."InvoiceDate" AS DATE)) = 2014
  GROUP BY i."CustomerID", DATE_TRUNC('month', CAST(i."InvoiceDate" AS DATE))
),
customer_averages AS (
  SELECT 
    "CustomerID",
    AVG(monthly_total) as avg_monthly_spending
  FROM monthly_totals
  GROUP BY "CustomerID"
)
SELECT MEDIAN(avg_monthly_spending) as median_avg_monthly_spending
FROM customer_averages