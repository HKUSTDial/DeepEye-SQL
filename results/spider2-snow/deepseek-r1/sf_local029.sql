SELECT 
    c."customer_unique_id",
    COUNT(DISTINCT o."order_id") as delivered_order_count,
    AVG(p."payment_value") as avg_payment_value,
    MIN(c."customer_city") as customer_city,
    MIN(c."customer_state") as customer_state
FROM 
    "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDERS" o
INNER JOIN 
    "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_CUSTOMERS" c 
    ON o."customer_id" = c."customer_id"
LEFT JOIN 
    "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_PAYMENTS" p 
    ON o."order_id" = p."order_id"
WHERE 
    o."order_status" = 'delivered'
GROUP BY 
    c."customer_unique_id"
ORDER BY 
    delivered_order_count DESC
LIMIT 3