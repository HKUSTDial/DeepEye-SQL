WITH user_purchases AS (
  SELECT 
    "user_id",
    TO_TIMESTAMP("created_at" / 1000000) AS purchase_ts
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
  WHERE "status" != 'Cancelled'
),
user_first_purchase AS (
  SELECT 
    "user_id",
    MIN(purchase_ts) AS first_purchase_ts
  FROM user_purchases
  GROUP BY "user_id"
),
cohort AS (
  SELECT 
    "user_id",
    first_purchase_ts
  FROM user_first_purchase
  WHERE EXTRACT(YEAR FROM first_purchase_ts) = 2020
    AND EXTRACT(MONTH FROM first_purchase_ts) = 1
),
returning_users AS (
  SELECT DISTINCT c."user_id"
  FROM cohort c
  INNER JOIN user_purchases up 
    ON c."user_id" = up."user_id"
  WHERE EXTRACT(YEAR FROM up.purchase_ts) = 2020
    AND EXTRACT(MONTH FROM up.purchase_ts) BETWEEN 2 AND 12
    AND up.purchase_ts > c.first_purchase_ts
)
SELECT 
  (SELECT COUNT(*) FROM returning_users) * 1.0 / (SELECT COUNT(*) FROM cohort) AS proportion_returned