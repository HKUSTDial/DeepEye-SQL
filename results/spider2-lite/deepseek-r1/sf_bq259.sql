WITH valid_purchases AS (
  SELECT DISTINCT
    "user_id",
    DATE_TRUNC('month', TO_TIMESTAMP("created_at" / 1000000)) AS purchase_month
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
  WHERE "status" != 'Cancelled'
    AND TO_TIMESTAMP("created_at" / 1000000) <= '2022-12-31 23:59:59'
),
user_first_month AS (
  SELECT
    "user_id",
    MIN(purchase_month) AS first_purchase_month
  FROM valid_purchases
  GROUP BY "user_id"
),
cohort_indicators AS (
  SELECT
    ufm."user_id",
    ufm.first_purchase_month,
    1 AS has_month0,
    CASE 
      WHEN DATEADD(month, 1, ufm.first_purchase_month) <= '2022-12-01' THEN
        CASE 
          WHEN EXISTS (SELECT 1 FROM valid_purchases vp WHERE vp."user_id" = ufm."user_id" AND vp.purchase_month = DATEADD(month, 1, ufm.first_purchase_month)) 
          THEN 1 
          ELSE 0 
        END
      ELSE NULL 
    END AS has_month1,
    CASE 
      WHEN DATEADD(month, 2, ufm.first_purchase_month) <= '2022-12-01' THEN
        CASE 
          WHEN EXISTS (SELECT 1 FROM valid_purchases vp WHERE vp."user_id" = ufm."user_id" AND vp.purchase_month = DATEADD(month, 2, ufm.first_purchase_month)) 
          THEN 1 
          ELSE 0 
        END
      ELSE NULL 
    END AS has_month2,
    CASE 
      WHEN DATEADD(month, 3, ufm.first_purchase_month) <= '2022-12-01' THEN
        CASE 
          WHEN EXISTS (SELECT 1 FROM valid_purchases vp WHERE vp."user_id" = ufm."user_id" AND vp.purchase_month = DATEADD(month, 3, ufm.first_purchase_month)) 
          THEN 1 
          ELSE 0 
        END
      ELSE NULL 
    END AS has_month3
  FROM user_first_month ufm
)
SELECT
  TO_CHAR(first_purchase_month, 'YYYY-MM') AS cohort_month,
  100.0 * SUM(has_month0) / COUNT(*) AS perc_first_month,
  100.0 * SUM(has_month1) / SUM(CASE WHEN has_month1 IS NOT NULL THEN 1 ELSE 0 END) AS perc_second_month,
  100.0 * SUM(has_month2) / SUM(CASE WHEN has_month2 IS NOT NULL THEN 1 ELSE 0 END) AS perc_third_month,
  100.0 * SUM(has_month3) / SUM(CASE WHEN has_month3 IS NOT NULL THEN 1 ELSE 0 END) AS perc_fourth_month
FROM cohort_indicators
GROUP BY first_purchase_month
ORDER BY first_purchase_month