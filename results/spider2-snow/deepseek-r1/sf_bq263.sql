WITH complete_orders_2023 AS (
    SELECT 
        "order_id",
        TO_TIMESTAMP("created_at" / 1000000) AS "order_timestamp"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
    WHERE "status" = 'Complete'
    AND EXTRACT(YEAR FROM TO_TIMESTAMP("created_at" / 1000000)) = 2023
),
sleep_lounge_order_items AS (
    SELECT 
        oi."order_id",
        oi."sale_price",
        ii."cost",
        o."order_timestamp"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
    JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p
        ON oi."product_id" = p."id"
    JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."INVENTORY_ITEMS" ii
        ON oi."inventory_item_id" = ii."id"
    JOIN complete_orders_2023 o
        ON oi."order_id" = o."order_id"
    WHERE p."category" = 'Sleep & Lounge'
    AND oi."status" = 'Complete'
)
SELECT 
    EXTRACT(MONTH FROM "order_timestamp") AS "month",
    SUM("sale_price") AS "total_sales",
    SUM("cost") AS "total_cost",
    COUNT(DISTINCT "order_id") AS "number_of_complete_orders",
    SUM("sale_price") - SUM("cost") AS "total_profit",
    CASE WHEN SUM("cost") > 0 
         THEN (SUM("sale_price") - SUM("cost")) / SUM("cost") 
         ELSE NULL 
    END AS "profit_to_cost_ratio"
FROM sleep_lounge_order_items
GROUP BY "month"
ORDER BY "month"