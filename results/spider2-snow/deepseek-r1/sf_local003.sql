WITH "order_payments_agg" AS (
    SELECT "order_id", SUM("payment_value") AS "order_payment"
    FROM "E_COMMERCE"."E_COMMERCE"."ORDER_PAYMENTS"
    GROUP BY "order_id"
),
"delivered_orders" AS (
    SELECT o."order_id", o."customer_id", o."order_purchase_timestamp",
           c."customer_unique_id", opa."order_payment"
    FROM "E_COMMERCE"."E_COMMERCE"."ORDERS" o
    INNER JOIN "E_COMMERCE"."E_COMMERCE"."CUSTOMERS" c 
        ON o."customer_id" = c."customer_id"
    INNER JOIN "order_payments_agg" opa 
        ON o."order_id" = opa."order_id"
    WHERE o."order_status" = 'delivered'
),
"customer_metrics" AS (
    SELECT "customer_unique_id",
           MAX(TO_TIMESTAMP("order_purchase_timestamp")) AS "last_purchase_ts",
           COUNT(DISTINCT "order_id") AS "frequency",
           SUM("order_payment") AS "monetary"
    FROM "delivered_orders"
    GROUP BY "customer_unique_id"
),
"reference_date" AS (
    SELECT MAX("last_purchase_ts") AS "ref_ts" FROM "customer_metrics"
),
"customer_metrics_with_recency" AS (
    SELECT cm.*, rd."ref_ts",
           DATEDIFF('day', cm."last_purchase_ts", rd."ref_ts") AS "recency_days"
    FROM "customer_metrics" cm
    CROSS JOIN "reference_date" rd
),
"customer_scores" AS (
    SELECT *,
           NTILE(5) OVER (ORDER BY "recency_days" ASC) AS "r_score",
           NTILE(5) OVER (ORDER BY "frequency" DESC) AS "f_score",
           NTILE(5) OVER (ORDER BY "monetary" DESC) AS "m_score"
    FROM "customer_metrics_with_recency"
),
"customer_segments" AS (
    SELECT *,
           "f_score" + "m_score" AS "f_m_sum",
           CASE 
                WHEN "r_score" = 1 AND "f_m_sum" BETWEEN 1 AND 4 THEN 'Champions'
                WHEN "r_score" IN (4,5) AND "f_m_sum" BETWEEN 1 AND 2 THEN 'Can''t Lose Them'
                WHEN "r_score" IN (4,5) AND "f_m_sum" BETWEEN 3 AND 6 THEN 'Hibernating'
                WHEN "r_score" IN (4,5) AND "f_m_sum" BETWEEN 7 AND 10 THEN 'Lost'
                WHEN "r_score" IN (2,3) AND "f_m_sum" BETWEEN 1 AND 4 THEN 'Loyal Customers'
                WHEN "r_score" = 3 AND "f_m_sum" BETWEEN 5 AND 6 THEN 'Needs Attention'
                WHEN "r_score" = 1 AND "f_m_sum" BETWEEN 7 AND 8 THEN 'Recent Users'
                WHEN ("r_score" = 1 AND "f_m_sum" BETWEEN 5 AND 6) OR ("r_score" = 2 AND "f_m_sum" BETWEEN 5 AND 8) THEN 'Potential Loyalists'
                WHEN "r_score" = 1 AND "f_m_sum" BETWEEN 9 AND 10 THEN 'Price Sensitive'
                WHEN "r_score" = 2 AND "f_m_sum" BETWEEN 9 AND 10 THEN 'Promising'
                WHEN "r_score" = 3 AND "f_m_sum" BETWEEN 7 AND 10 THEN 'About to Sleep'
                ELSE 'Other'
           END AS "rfm_segment"
    FROM "customer_scores"
)
SELECT 
    "rfm_segment",
    COUNT("customer_unique_id") AS "customer_count",
    SUM("frequency") AS "total_orders",
    SUM("monetary") AS "total_spend",
    SUM("monetary") / SUM("frequency") AS "avg_sales_per_order"
FROM "customer_segments"
GROUP BY "rfm_segment"
ORDER BY "rfm_segment"