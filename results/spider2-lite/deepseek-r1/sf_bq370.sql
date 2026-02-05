WITH order_lines AS (
    SELECT 
        so."CustomerID",
        so."OrderID",
        sol."StockItemID",
        sol."Quantity",
        sol."UnitPrice",
        sol."TaxRate",
        sol."Quantity" * sol."UnitPrice" * (1 + sol."TaxRate" / 100) AS OrderExtendedPrice
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_ORDERS" so
    JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_ORDERLINES" sol
        ON so."OrderID" = sol."OrderID"
),
invoice_lines AS (
    SELECT 
        si."CustomerID",
        si."InvoiceID",
        sil."StockItemID",
        sil."Quantity",
        sil."UnitPrice",
        sil."TaxRate",
        sil."ExtendedPrice" AS InvoiceExtendedPrice
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICES" si
    JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICELINES" sil
        ON si."InvoiceID" = sil."InvoiceID"
),
order_aggregates AS (
    SELECT 
        "CustomerID",
        "StockItemID",
        "Quantity",
        "UnitPrice",
        "TaxRate",
        COUNT(*) AS order_line_count,
        SUM(OrderExtendedPrice) AS order_total_value
    FROM order_lines
    GROUP BY "CustomerID", "StockItemID", "Quantity", "UnitPrice", "TaxRate"
),
invoice_aggregates AS (
    SELECT 
        "CustomerID",
        "StockItemID",
        "Quantity",
        "UnitPrice",
        "TaxRate",
        COUNT(*) AS invoice_line_count,
        SUM(InvoiceExtendedPrice) AS invoice_total_value
    FROM invoice_lines
    GROUP BY "CustomerID", "StockItemID", "Quantity", "UnitPrice", "TaxRate"
),
mismatch_combinations AS (
    SELECT 
        COALESCE(oa."CustomerID", ia."CustomerID") AS "CustomerID",
        COALESCE(oa."StockItemID", ia."StockItemID") AS "StockItemID",
        COALESCE(oa."Quantity", ia."Quantity") AS "Quantity",
        COALESCE(oa."UnitPrice", ia."UnitPrice") AS "UnitPrice",
        COALESCE(oa."TaxRate", ia."TaxRate") AS "TaxRate"
    FROM order_aggregates oa
    FULL OUTER JOIN invoice_aggregates ia
        ON oa."CustomerID" = ia."CustomerID"
        AND oa."StockItemID" = ia."StockItemID"
        AND oa."Quantity" = ia."Quantity"
        AND oa."UnitPrice" = ia."UnitPrice"
        AND oa."TaxRate" = ia."TaxRate"
    WHERE COALESCE(oa.order_line_count, 0) <> COALESCE(ia.invoice_line_count, 0)
),
customers_with_mismatch AS (
    SELECT DISTINCT "CustomerID" FROM mismatch_combinations
),
customer_order_counts AS (
    SELECT 
        "CustomerID",
        COUNT(DISTINCT "OrderID") AS distinct_orders
    FROM order_lines
    GROUP BY "CustomerID"
),
customer_invoice_counts AS (
    SELECT 
        "CustomerID",
        COUNT(DISTINCT "InvoiceID") AS distinct_invoices
    FROM invoice_lines
    GROUP BY "CustomerID"
),
customer_counts AS (
    SELECT 
        coc."CustomerID",
        coc.distinct_orders,
        cic.distinct_invoices
    FROM customer_order_counts coc
    JOIN customer_invoice_counts cic
        ON coc."CustomerID" = cic."CustomerID"
),
eligible_customers AS (
    SELECT cc."CustomerID"
    FROM customer_counts cc
    WHERE cc.distinct_orders = cc.distinct_invoices
        AND NOT EXISTS (SELECT 1 FROM customers_with_mismatch cm WHERE cm."CustomerID" = cc."CustomerID")
)
SELECT COUNT(*) AS customer_count FROM eligible_customers