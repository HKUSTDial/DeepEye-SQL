WITH invoice_totals AS (
  SELECT 
    i."InvoiceID",
    TO_DATE(i."InvoiceDate") AS invoice_date,
    SUM(l."Quantity" * l."UnitPrice") AS invoice_total
  FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICES" i
  INNER JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICELINES" l
    ON i."InvoiceID" = l."InvoiceID"
  WHERE EXTRACT(YEAR FROM TO_DATE(i."InvoiceDate")) = 2013
  GROUP BY i."InvoiceID", i."InvoiceDate"
),
quarter_avg AS (
  SELECT 
    QUARTER(invoice_date) AS quarter,
    AVG(invoice_total) AS avg_invoice_value
  FROM invoice_totals
  GROUP BY quarter
)
SELECT 
  MAX(avg_invoice_value) - MIN(avg_invoice_value) AS difference
FROM quarter_avg