WITH first_purchase AS (
    SELECT "customer_id", 
           MIN(TO_TIMESTAMP("payment_date")) AS first_purchase_ts,
           SUM("amount") AS ltv
    FROM "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT"
    GROUP BY "customer_id"
    HAVING SUM("amount") > 0
),
customer_windows AS (
    SELECT fp."customer_id",
           fp.ltv,
           SUM(CASE WHEN TO_TIMESTAMP(p."payment_date") <= DATEADD(hour, 168, fp.first_purchase_ts) THEN p."amount" ELSE 0 END) AS sum_7d,
           SUM(CASE WHEN TO_TIMESTAMP(p."payment_date") <= DATEADD(hour, 720, fp.first_purchase_ts) THEN p."amount" ELSE 0 END) AS sum_30d
    FROM first_purchase fp
    INNER JOIN "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT" p ON fp."customer_id" = p."customer_id"
    GROUP BY fp."customer_id", fp.ltv, fp.first_purchase_ts
)
SELECT AVG(ltv) AS avg_ltv,
       AVG((sum_7d / ltv) * 100) AS avg_pct_7days,
       AVG((sum_30d / ltv) * 100) AS avg_pct_30days
FROM customer_windows;