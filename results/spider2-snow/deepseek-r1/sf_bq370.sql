WITH OrderLinesAgg AS (
    SELECT 
        so."CustomerID",
        sol."StockItemID",
        sol."Quantity",
        sol."UnitPrice",
        sol."TaxRate",
        COUNT(*) as "LineCount",
        SUM(sol."UnitPrice" * sol."Quantity" * (1 + sol."TaxRate"/100)) as "OrderLineTotal"
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_ORDERS" so
    JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_ORDERLINES" sol ON so."OrderID" = sol."OrderID"
    GROUP BY so."CustomerID", sol."StockItemID", sol."Quantity", sol."UnitPrice", sol."TaxRate"
),
InvoiceLinesAgg AS (
    SELECT 
        si."CustomerID",
        sil."StockItemID",
        sil."Quantity",
        sil."UnitPrice",
        sil."TaxRate",
        COUNT(*) as "LineCount",
        SUM(sil."ExtendedPrice") as "InvoiceLineTotal"
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICES" si
    JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICELINES" sil ON si."InvoiceID" = sil."InvoiceID"
    GROUP BY si."CustomerID", sil."StockItemID", sil."Quantity", sil."UnitPrice", sil."TaxRate"
),
OrderSummary AS (
    SELECT 
        so."CustomerID",
        COUNT(DISTINCT so."OrderID") as "OrderCount",
        SUM(sol."UnitPrice" * sol."Quantity" * (1 + sol."TaxRate"/100)) as "TotalOrderValue"
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_ORDERS" so
    JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_ORDERLINES" sol ON so."OrderID" = sol."OrderID"
    GROUP BY so."CustomerID"
),
InvoiceSummary AS (
    SELECT 
        si."CustomerID",
        COUNT(DISTINCT si."InvoiceID") as "InvoiceCount",
        SUM(sil."ExtendedPrice") as "TotalInvoiceValue"
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICES" si
    JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICELINES" sil ON si."InvoiceID" = sil."InvoiceID"
    GROUP BY si."CustomerID"
),
LineMismatch AS (
    SELECT COALESCE(ola."CustomerID", ila."CustomerID") as "CustomerID"
    FROM OrderLinesAgg ola
    FULL OUTER JOIN InvoiceLinesAgg ila ON ola."CustomerID" = ila."CustomerID" 
        AND ola."StockItemID" = ila."StockItemID"
        AND ola."Quantity" = ila."Quantity"
        AND ola."UnitPrice" = ila."UnitPrice"
        AND ola."TaxRate" = ila."TaxRate"
    WHERE ola."CustomerID" IS NULL OR ila."CustomerID" IS NULL 
       OR ola."LineCount" != ila."LineCount"
       OR ABS(ola."OrderLineTotal" - ila."InvoiceLineTotal") > 0.001
)
SELECT COUNT(DISTINCT os."CustomerID") as "CustomerCount"
FROM OrderSummary os
JOIN InvoiceSummary ins ON os."CustomerID" = ins."CustomerID"
LEFT JOIN LineMismatch lm ON os."CustomerID" = lm."CustomerID"
WHERE os."OrderCount" = ins."InvoiceCount"
  AND ABS(os."TotalOrderValue" - ins."TotalInvoiceValue") <= 0.001
  AND lm."CustomerID" IS NULL