WITH store_stats AS (
  SELECT 
    o."store_id",
    s."store_name",
    COUNT(DISTINCT o."order_id") AS "total_orders",
    COUNT(DISTINCT CASE WHEN d."delivery_status" = 'DELIVERED' THEN o."order_id" END) AS "delivered_orders"
  FROM "DELIVERY_CENTER"."DELIVERY_CENTER"."ORDERS" o
  JOIN "DELIVERY_CENTER"."DELIVERY_CENTER"."STORES" s ON o."store_id" = s."store_id"
  LEFT JOIN "DELIVERY_CENTER"."DELIVERY_CENTER"."DELIVERIES" d ON o."delivery_order_id" = d."delivery_order_id"
  GROUP BY o."store_id", s."store_name"
)
SELECT 
  "store_id",
  "store_name",
  "total_orders",
  "delivered_orders",
  "delivered_orders" / NULLIF("total_orders", 0) AS ratio
FROM store_stats
ORDER BY "total_orders" DESC
LIMIT 1