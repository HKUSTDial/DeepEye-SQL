WITH hub_month_counts AS (
    SELECT 
        h."hub_id",
        h."hub_name",
        COUNT(CASE WHEN o."order_created_month" = 2 THEN 1 END) AS feb_count,
        COUNT(CASE WHEN o."order_created_month" = 3 THEN 1 END) AS mar_count
    FROM "DELIVERY_CENTER"."DELIVERY_CENTER"."ORDERS" o
    JOIN "DELIVERY_CENTER"."DELIVERY_CENTER"."STORES" s ON o."store_id" = s."store_id"
    JOIN "DELIVERY_CENTER"."DELIVERY_CENTER"."HUBS" h ON s."hub_id" = h."hub_id"
    WHERE o."order_status" = 'FINISHED'
        AND o."order_created_year" = 2021
        AND o."order_created_month" IN (2, 3)
    GROUP BY h."hub_id", h."hub_name"
)
SELECT "hub_id", "hub_name", feb_count, mar_count
FROM hub_month_counts
WHERE feb_count > 0 AND (mar_count - feb_count) / NULLIF(feb_count, 0) > 0.2