WITH RECURSIVE months AS (
    SELECT TO_DATE('2019-01-01', 'YYYY-MM-DD') AS month_start
    UNION ALL
    SELECT DATEADD(month, 1, month_start) AS month_start
    FROM months
    WHERE month_start < TO_DATE('2019-12-01', 'YYYY-MM-DD')
), product_info AS (
    SELECT 
        pm."product_id",
        pm."qty_minimum",
        pm."qty_purchase",
        COALESCE(SUM(i."qty"), 0) AS start_inv
    FROM "ORACLE_SQL"."ORACLE_SQL"."PRODUCT_MINIMUMS" pm
    LEFT JOIN "ORACLE_SQL"."ORACLE_SQL"."INVENTORY" i ON pm."product_id" = i."product_id"
    GROUP BY pm."product_id", pm."qty_minimum", pm."qty_purchase"
), inventory_trajectory AS (
    SELECT 
        pi."product_id",
        pi."qty_minimum",
        pi."qty_purchase",
        pi.start_inv AS prev_ending_inv,
        TO_DATE('2019-01-01', 'YYYY-MM-DD') AS month_start,
        COALESCE(ms."qty", 0) AS sales_qty,
        COALESCE(mb."qty", 0) AS budget_qty,
        pi.start_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0) AS inv_after,
        CASE 
            WHEN (pi.start_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)) < pi."qty_minimum" 
            THEN (pi.start_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)) + pi."qty_purchase"
            ELSE pi.start_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)
        END AS ending_inv,
        ABS(CASE 
            WHEN (pi.start_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)) < pi."qty_minimum" 
            THEN (pi.start_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)) + pi."qty_purchase"
            ELSE pi.start_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)
        END - pi."qty_minimum") AS abs_diff
    FROM product_info pi
    LEFT JOIN "ORACLE_SQL"."ORACLE_SQL"."MONTHLY_SALES" ms 
        ON pi."product_id" = ms."product_id" 
        AND TO_DATE(ms."mth", 'YYYY-MM-DD') = TO_DATE('2019-01-01', 'YYYY-MM-DD')
    LEFT JOIN "ORACLE_SQL"."ORACLE_SQL"."MONTHLY_BUDGET" mb 
        ON pi."product_id" = mb."product_id" 
        AND TO_DATE(mb."mth", 'YYYY-MM-DD') = TO_DATE('2019-01-01', 'YYYY-MM-DD')
    UNION ALL
    SELECT 
        it."product_id",
        it."qty_minimum",
        it."qty_purchase",
        it.ending_inv AS prev_ending_inv,
        DATEADD(month, 1, it.month_start) AS month_start,
        COALESCE(ms."qty", 0) AS sales_qty,
        COALESCE(mb."qty", 0) AS budget_qty,
        it.ending_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0) AS inv_after,
        CASE 
            WHEN (it.ending_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)) < it."qty_minimum" 
            THEN (it.ending_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)) + it."qty_purchase"
            ELSE it.ending_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)
        END AS ending_inv,
        ABS(CASE 
            WHEN (it.ending_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)) < it."qty_minimum" 
            THEN (it.ending_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)) + it."qty_purchase"
            ELSE it.ending_inv - COALESCE(ms."qty", 0) + COALESCE(mb."qty", 0)
        END - it."qty_minimum") AS abs_diff
    FROM inventory_trajectory it
    LEFT JOIN "ORACLE_SQL"."ORACLE_SQL"."MONTHLY_SALES" ms 
        ON it."product_id" = ms."product_id" 
        AND TO_DATE(ms."mth", 'YYYY-MM-DD') = DATEADD(month, 1, it.month_start)
    LEFT JOIN "ORACLE_SQL"."ORACLE_SQL"."MONTHLY_BUDGET" mb 
        ON it."product_id" = mb."product_id" 
        AND TO_DATE(mb."mth", 'YYYY-MM-DD') = DATEADD(month, 1, it.month_start)
    WHERE it.month_start < TO_DATE('2019-12-01', 'YYYY-MM-DD')
)
SELECT 
    product_id,
    month_start,
    abs_diff
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY abs_diff, month_start) AS rn
    FROM inventory_trajectory
) ranked
WHERE rn = 1