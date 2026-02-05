SELECT
    AVG("total_payments") AS "avg_total_payments",
    AVG("total_delivered_order_counts") AS "avg_delivered_order_counts"
FROM (
    SELECT
        "city",
        "total_payments",
        "total_delivered_order_counts"
    FROM (
        SELECT
            c."customer_city" AS "city",
            SUM(p."payment_value") AS "total_payments",
            COUNT(DISTINCT o."order_id") AS "total_delivered_order_counts"
        FROM "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDERS" o
        INNER JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_CUSTOMERS" c ON o."customer_id" = c."customer_id"
        INNER JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_PAYMENTS" p ON o."order_id" = p."order_id"
        WHERE o."order_status" = 'delivered'
        GROUP BY c."customer_city"
    ) AS "city_totals"
    ORDER BY "total_payments" ASC
    LIMIT 5
) AS "bottom_five"