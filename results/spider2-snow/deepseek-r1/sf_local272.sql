WITH order_lines_cte AS (
  SELECT 
    "id" AS "order_line_id",
    "product_id",
    "qty" AS "line_qty",
    SUM("qty") OVER (PARTITION BY "product_id" ORDER BY "id") AS "cum_qty",
    "cum_qty" - "qty" AS "cum_qty_prev"
  FROM "ORACLE_SQL"."ORACLE_SQL"."ORDERLINES"
  WHERE "order_id" = 423
),
inventory_cte AS (
  SELECT 
    i."product_id",
    i."location_id",
    i."qty" AS "inv_qty",
    p."purchased" AS "purchase_date",
    SUM(i."qty") OVER (PARTITION BY i."product_id" ORDER BY p."purchased", i."qty") AS "cum_inv_qty",
    "cum_inv_qty" - i."qty" AS "cum_inv_prev"
  FROM "ORACLE_SQL"."ORACLE_SQL"."INVENTORY" i
  INNER JOIN "ORACLE_SQL"."ORACLE_SQL"."LOCATIONS" l 
    ON i."location_id" = l."id"
  INNER JOIN "ORACLE_SQL"."ORACLE_SQL"."PURCHASES" p 
    ON i."purchase_id" = p."id"
  WHERE l."warehouse" = 1
)
SELECT 
  ol."order_line_id",
  ol."product_id",
  l."aisle",
  l."position",
  LEAST(ol."cum_qty", inv."cum_inv_qty") - GREATEST(ol."cum_qty_prev", inv."cum_inv_prev") AS "qty_to_pick"
FROM order_lines_cte ol
INNER JOIN inventory_cte inv 
  ON ol."product_id" = inv."product_id"
  AND ol."cum_qty_prev" < inv."cum_inv_qty" 
  AND ol."cum_qty" > inv."cum_inv_prev"
INNER JOIN "ORACLE_SQL"."ORACLE_SQL"."LOCATIONS" l 
  ON inv."location_id" = l."id"
WHERE LEAST(ol."cum_qty", inv."cum_inv_qty") - GREATEST(ol."cum_qty_prev", inv."cum_inv_prev") > 0
ORDER BY ol."order_line_id", inv."purchase_date", inv."inv_qty"