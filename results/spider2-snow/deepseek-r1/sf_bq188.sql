WITH top_category AS (
  SELECT p."category", COUNT(*) as total_quantity
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p 
    ON oi."product_id" = p."id"
  WHERE oi."status" IN ('Complete', 'Shipped')
  GROUP BY p."category"
  ORDER BY total_quantity DESC
  LIMIT 1
),
product_page_events AS (
  SELECT 
    e."session_id",
    e."created_at",
    TRY_CAST(SPLIT_PART(SPLIT_PART(e."uri", '/', 3), '?', 1) AS NUMBER) as extracted_product_id
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."EVENTS" e
  WHERE e."uri" LIKE '/product/%' OR e."uri" LIKE '/products/%'
),
categorized_events AS (
  SELECT 
    ppe."session_id",
    ppe."created_at",
    p."category"
  FROM product_page_events ppe
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p 
    ON ppe.extracted_product_id = p."id"
  WHERE ppe.extracted_product_id IS NOT NULL
),
session_events_with_next AS (
  SELECT
    ce."session_id",
    ce."created_at",
    ce."category",
    LEAD(ce."created_at") OVER (
      PARTITION BY ce."session_id" 
      ORDER BY ce."created_at"
    ) as "next_event_created_at"
  FROM categorized_events ce
),
filtered_events AS (
  SELECT 
    sewn."session_id",
    sewn."created_at",
    sewn."next_event_created_at"
  FROM session_events_with_next sewn
  JOIN top_category tc ON sewn."category" = tc."category"
  WHERE sewn."next_event_created_at" IS NOT NULL
),
time_differences AS (
  SELECT
    ("next_event_created_at" - "created_at") / 60000000 as time_spent_minutes
  FROM filtered_events
)
SELECT 
  (SELECT "category" FROM top_category) as top_category,
  AVG(time_spent_minutes) as average_time_spent_minutes
FROM time_differences