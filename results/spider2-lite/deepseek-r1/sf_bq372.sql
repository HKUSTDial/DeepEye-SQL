WITH lost_orders AS (
    SELECT 
        so."OrderID",
        sc."CustomerCategoryID",
        SUM(sol."Quantity" * sol."UnitPrice" * (1 + sol."TaxRate" / 100)) AS "order_value"
    FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_ORDERS" so
    INNER JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_ORDERLINES" sol ON so."OrderID" = sol."OrderID"
    INNER JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_CUSTOMERS" sc ON so."CustomerID" = sc."CustomerID"
    WHERE NOT EXISTS (
        SELECT 1 
        FROM "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_INVOICES" si 
        WHERE si."OrderID" = so."OrderID"
    )
    GROUP BY so."OrderID", sc."CustomerCategoryID"
),
category_max AS (
    SELECT 
        "CustomerCategoryID",
        MAX("order_value") AS "max_lost_order_value"
    FROM lost_orders
    GROUP BY "CustomerCategoryID"
),
overall_avg AS (
    SELECT AVG("max_lost_order_value") AS "avg_max_lost"
    FROM category_max
)
SELECT 
    cc."CustomerCategoryName",
    cm."max_lost_order_value"
FROM category_max cm
CROSS JOIN overall_avg oa
INNER JOIN "WIDE_WORLD_IMPORTERS"."WIDE_WORLD_IMPORTERS"."SALES_CUSTOMERCATEGORIES" cc ON cm."CustomerCategoryID" = cc."CustomerCategoryID"
ORDER BY ABS(cm."max_lost_order_value" - oa."avg_max_lost")
LIMIT 1