WITH batch_events AS (
  SELECT 
    i."product_id",
    p."purchased" AS event_date,
    i."qty" AS event_qty,
    1 AS event_type_order,
    i."qty" AS sort_key2,
    NULL AS orderline_id,
    'add' AS event_type
  FROM "ORACLE_SQL"."ORACLE_SQL"."INVENTORY" i
  JOIN "ORACLE_SQL"."ORACLE_SQL"."PURCHASES" p ON i."purchase_id" = p."id"
),
demand_events AS (
  SELECT 
    ol."product_id",
    o."ordered" AS event_date,
    -ol."qty" AS event_qty,
    2 AS event_type_order,
    ol."id" AS sort_key2,
    ol."id" AS orderline_id,
    'demand' AS event_type
  FROM "ORACLE_SQL"."ORACLE_SQL"."ORDERLINES" ol
  JOIN "ORACLE_SQL"."ORACLE_SQL"."ORDERS" o ON ol."order_id" = o."id"
),
events AS (
  SELECT * FROM batch_events
  UNION ALL
  SELECT * FROM demand_events
),
sorted_events AS (
  SELECT 
    "product_id",
    "event_date",
    "event_qty",
    "event_type",
    "orderline_id",
    ROW_NUMBER() OVER (PARTITION BY "product_id" ORDER BY "event_date", "event_type_order", "sort_key2") AS seq
  FROM events
),
recursive_inv AS (
  SELECT 
    seq,
    "product_id",
    "event_type",
    "event_qty",
    "orderline_id",
    CASE WHEN "event_type" = 'add' THEN "event_qty" ELSE 0 END AS inv_added,
    CASE WHEN "event_type" = 'demand' THEN "event_qty" ELSE 0 END AS demand_qty,
    0 AS fulfilled_qty,
    CASE WHEN "event_type" = 'add' THEN "event_qty" ELSE 0 END AS inv_after
  FROM sorted_events
  WHERE seq = 1
  UNION ALL
  SELECT 
    e.seq,
    e."product_id",
    e."event_type",
    e."event_qty",
    e."orderline_id",
    CASE WHEN e."event_type" = 'add' THEN e."event_qty" ELSE 0 END,
    CASE WHEN e."event_type" = 'demand' THEN e."event_qty" ELSE 0 END,
    CASE WHEN e."event_type" = 'demand' THEN LEAST(ABS(e."event_qty"), r.inv_after) ELSE 0 END,
    CASE WHEN e."event_type" = 'add' THEN r.inv_after + e."event_qty"
         WHEN e."event_type" = 'demand' THEN r.inv_after - LEAST(ABS(e."event_qty"), r.inv_after)
    END
  FROM sorted_events e
  JOIN recursive_inv r ON e."product_id" = r."product_id" AND e.seq = r.seq + 1
),
pick_percentages AS (
  SELECT 
    "product_id",
    "orderline_id",
    fulfilled_qty,
    ABS("event_qty") AS original_demand,
    CASE WHEN ABS("event_qty") > 0 THEN fulfilled_qty / ABS("event_qty") ELSE 0 END AS pick_pct
  FROM recursive_inv
  WHERE "event_type" = 'demand'
)
SELECT 
  p."name" AS product_name,
  AVG(pp.pick_pct) AS average_pick_percentage
FROM pick_percentages pp
JOIN "ORACLE_SQL"."ORACLE_SQL"."PRODUCTS" p ON pp."product_id" = p."id"
GROUP BY p."name"
ORDER BY p."name"