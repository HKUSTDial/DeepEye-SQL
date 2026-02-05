WITH category_totals AS (
  SELECT 
    p."category",
    COUNT(*) as total_quantity
  FROM 
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  JOIN 
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p 
    ON oi."product_id" = p."id"
  WHERE 
    oi."status" IN ('Complete', 'Shipped') 
    AND oi."returned_at" IS NULL
  GROUP BY 
    p."category"
),
top_category AS (
  SELECT 
    "category"
  FROM 
    category_totals
  ORDER BY 
    total_quantity DESC
  LIMIT 1
),
product_events AS (
  SELECT 
    e."session_id",
    e."created_at",
    TRY_CAST(SPLIT_PART(e."uri", '/', 3) AS NUMBER) as "product_id"
  FROM 
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."EVENTS" e
  WHERE 
    e."uri" LIKE '/product/%'
),
events_with_next AS (
  SELECT 
    pe."session_id",
    pe."created_at",
    pe."product_id",
    LEAD(pe."created_at") OVER (PARTITION BY pe."session_id" ORDER BY pe."created_at") as "next_created_at"
  FROM 
    product_events pe
),
valid_events AS (
  SELECT 
    ewn."created_at",
    ewn."next_created_at",
    p."category"
  FROM 
    events_with_next ewn
  JOIN 
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p 
    ON ewn."product_id" = p."id"
  WHERE 
    ewn."next_created_at" IS NOT NULL
)
SELECT 
  tc."category",
  AVG((ve."next_created_at" - ve."created_at") / 60000000.0) as average_time_minutes
FROM 
  top_category tc
JOIN 
  valid_events ve 
  ON tc."category" = ve."category"
GROUP BY 
  tc."category"