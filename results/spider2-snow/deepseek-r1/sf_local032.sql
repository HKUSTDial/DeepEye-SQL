WITH delivered_orders AS (
    SELECT "order_id", "customer_id"
    FROM "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDERS"
    WHERE "order_status" = 'delivered'
),
seller_customer_count AS (
    SELECT oi."seller_id", COUNT(DISTINCT c."customer_unique_id") AS value
    FROM delivered_orders do
    JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_ITEMS" oi ON do."order_id" = oi."order_id"
    JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_CUSTOMERS" c ON do."customer_id" = c."customer_id"
    GROUP BY oi."seller_id"
),
top_seller_customers AS (
    SELECT "seller_id", value,
           ROW_NUMBER() OVER (ORDER BY value DESC) AS rn
    FROM seller_customer_count
),
seller_profit AS (
    SELECT oi."seller_id", SUM(oi."price" - oi."freight_value") AS value
    FROM delivered_orders do
    JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_ITEMS" oi ON do."order_id" = oi."order_id"
    GROUP BY oi."seller_id"
),
top_seller_profit AS (
    SELECT "seller_id", value,
           ROW_NUMBER() OVER (ORDER BY value DESC) AS rn
    FROM seller_profit
),
seller_order_count AS (
    SELECT oi."seller_id", COUNT(DISTINCT do."order_id") AS value
    FROM delivered_orders do
    JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_ITEMS" oi ON do."order_id" = oi."order_id"
    GROUP BY oi."seller_id"
),
top_seller_orders AS (
    SELECT "seller_id", value,
           ROW_NUMBER() OVER (ORDER BY value DESC) AS rn
    FROM seller_order_count
),
seller_five_star_count AS (
    SELECT oi."seller_id", COUNT(DISTINCT r."review_id") AS value
    FROM delivered_orders do
    JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_REVIEWS" r ON do."order_id" = r."order_id"
    JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_ITEMS" oi ON do."order_id" = oi."order_id"
    WHERE r."review_score" = 5
    GROUP BY oi."seller_id"
),
top_seller_five_star AS (
    SELECT "seller_id", value,
           ROW_NUMBER() OVER (ORDER BY value DESC) AS rn
    FROM seller_five_star_count
)
SELECT 'Seller with highest number of distinct customer unique IDs' AS achievement_description,
       "seller_id", value
FROM top_seller_customers WHERE rn = 1
UNION ALL
SELECT 'Seller with highest profit' AS achievement_description,
       "seller_id", value
FROM top_seller_profit WHERE rn = 1
UNION ALL
SELECT 'Seller with highest number of distinct orders' AS achievement_description,
       "seller_id", value
FROM top_seller_orders WHERE rn = 1
UNION ALL
SELECT 'Seller with most 5-star ratings' AS achievement_description,
       "seller_id", value
FROM top_seller_five_star WHERE rn = 1