WITH deliveries_with_date AS (
    SELECT
        d."driver_id" AS "driver_id",
        TO_DATE(o."order_moment_delivering", 'MM/DD/YYYY HH12:MI:SS AM') AS "delivery_date"
    FROM "DELIVERY_CENTER"."DELIVERY_CENTER"."DELIVERIES" d
    INNER JOIN "DELIVERY_CENTER"."DELIVERY_CENTER"."ORDERS" o
        ON d."delivery_order_id" = o."delivery_order_id"
    WHERE o."order_moment_delivering" IS NOT NULL
        AND o."order_moment_delivering" != ''
        AND d."delivery_status" = 'DELIVERED'
),
deliveries_per_day AS (
    SELECT
        "driver_id",
        "delivery_date",
        COUNT(*) AS "num_deliveries"
    FROM deliveries_with_date
    GROUP BY "driver_id", "delivery_date"
),
driver_avg AS (
    SELECT
        "driver_id",
        AVG("num_deliveries") AS "avg_daily_deliveries"
    FROM deliveries_per_day
    GROUP BY "driver_id"
)
SELECT
    "driver_id",
    "avg_daily_deliveries"
FROM driver_avg
ORDER BY "avg_daily_deliveries" DESC
LIMIT 5