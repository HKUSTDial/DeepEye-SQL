WITH completed_orders AS (
    SELECT 
        o."order_id",
        DATE_TRUNC('MONTH', TO_TIMESTAMP(o."delivered_at" / 1000000)) AS delivery_month
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o
    WHERE o."status" = 'Complete'
        AND o."delivered_at" IS NOT NULL
        AND TO_TIMESTAMP(o."delivered_at" / 1000000) < '2022-01-01'
),
order_details AS (
    SELECT 
        p."category",
        EXTRACT(YEAR FROM co.delivery_month) AS delivery_year,
        EXTRACT(MONTH FROM co.delivery_month) AS delivery_month,
        co."order_id",
        oi."sale_price",
        p."cost"
    FROM completed_orders co
    INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
        ON co."order_id" = oi."order_id"
    INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p
        ON oi."product_id" = p."id"
),
monthly_aggregates AS (
    SELECT 
        "category",
        delivery_year,
        delivery_month,
        SUM("sale_price") AS total_revenue,
        COUNT(DISTINCT "order_id") AS total_orders,
        SUM("cost") AS total_cost
    FROM order_details
    GROUP BY "category", delivery_year, delivery_month
),
with_previous_month AS (
    SELECT 
        "category",
        delivery_year,
        delivery_month,
        total_revenue,
        total_orders,
        total_cost,
        LAG(total_revenue) OVER (PARTITION BY "category" ORDER BY delivery_year, delivery_month) AS prev_revenue,
        LAG(total_orders) OVER (PARTITION BY "category" ORDER BY delivery_year, delivery_month) AS prev_orders
    FROM monthly_aggregates
)
SELECT 
    "category",
    delivery_year,
    delivery_month,
    total_revenue,
    total_orders,
    CASE 
        WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN NULL
        ELSE ROUND(((total_revenue - prev_revenue) / prev_revenue) * 100, 2)
    END AS revenue_growth_pct,
    CASE 
        WHEN prev_orders IS NULL OR prev_orders = 0 THEN NULL
        ELSE ROUND(((total_orders - prev_orders) / prev_orders) * 100, 2)
    END AS orders_growth_pct,
    total_cost,
    total_revenue - total_cost AS total_profit,
    CASE 
        WHEN total_cost = 0 THEN NULL
        ELSE ROUND((total_revenue - total_cost) / total_cost, 4)
    END AS profit_to_cost_ratio
FROM with_previous_month
ORDER BY "category", delivery_year, delivery_month