WITH order_totals AS (
    SELECT "order_id", SUM("payment_value") AS order_total
    FROM "E_COMMERCE"."E_COMMERCE"."ORDER_PAYMENTS"
    GROUP BY "order_id"
),
customer_orders AS (
    SELECT 
        o."customer_id",
        o."order_id",
        ot.order_total,
        TO_TIMESTAMP(o."order_purchase_timestamp") AS purchase_ts
    FROM "E_COMMERCE"."E_COMMERCE"."ORDERS" o
    JOIN order_totals ot ON o."order_id" = ot."order_id"
),
customer_aggregates AS (
    SELECT 
        "customer_id",
        COUNT(DISTINCT "order_id") AS num_orders,
        AVG(order_total) AS avg_payment_per_order,
        MIN(purchase_ts) AS earliest_purchase,
        MAX(purchase_ts) AS latest_purchase
    FROM customer_orders
    GROUP BY "customer_id"
),
customer_lifespan AS (
    SELECT 
        *,
        DATEDIFF(day, earliest_purchase, latest_purchase) AS days_diff,
        CASE 
            WHEN days_diff / 7.0 < 1.0 THEN 1.0 
            ELSE days_diff / 7.0 
        END AS lifespan_weeks
    FROM customer_aggregates
),
ranked_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY avg_payment_per_order DESC) AS rn
    FROM customer_lifespan
)
SELECT 
    "customer_id",
    num_orders AS number_of_orders,
    avg_payment_per_order AS average_payment_per_order,
    lifespan_weeks AS customer_lifespan_in_weeks
FROM ranked_customers
WHERE rn <= 3
ORDER BY rn