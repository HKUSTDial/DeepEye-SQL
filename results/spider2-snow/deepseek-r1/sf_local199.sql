WITH "rental_store" AS (
  SELECT 
    "s"."store_id" AS "store_id",
    EXTRACT(YEAR FROM CAST("r"."rental_date" AS TIMESTAMP)) AS "year",
    EXTRACT(MONTH FROM CAST("r"."rental_date" AS TIMESTAMP)) AS "month"
  FROM "SQLITE_SAKILA"."SQLITE_SAKILA"."RENTAL" AS "r"
  JOIN "SQLITE_SAKILA"."SQLITE_SAKILA"."STAFF" AS "s"
    ON "r"."staff_id" = "s"."staff_id"
),
"aggregated" AS (
  SELECT 
    "store_id",
    "year",
    "month",
    COUNT(*) AS "total_rentals"
  FROM "rental_store"
  GROUP BY "store_id", "year", "month"
),
"ranked" AS (
  SELECT 
    "store_id",
    "year",
    "month",
    "total_rentals",
    RANK() OVER (PARTITION BY "store_id" ORDER BY "total_rentals" DESC) AS "rnk"
  FROM "aggregated"
)
SELECT 
  "store_id",
  "year",
  "month",
  "total_rentals"
FROM "ranked"
WHERE "rnk" = 1
ORDER BY "store_id"