WITH invoice_totals AS (
    SELECT i."InvoiceID", TO_DATE(i."InvoiceDate", 'YYYY-MM-DD') AS invoice_date, SUM(l."UnitPrice" * l."Quantity") AS total_value
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICES" i
    JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICELINES" l ON i."InvoiceID" = l."InvoiceID"
    WHERE EXTRACT(YEAR FROM TO_DATE(i."InvoiceDate", 'YYYY-MM-DD')) = 2013
    GROUP BY i."InvoiceID", i."InvoiceDate"
), quarterly_averages AS (
    SELECT EXTRACT(QUARTER FROM invoice_date) AS quarter, AVG(total_value) AS avg_invoice_value
    FROM invoice_totals
    GROUP BY quarter
)
SELECT MAX(avg_invoice_value) - MIN(avg_invoice_value) AS difference
FROM quarterly_averages