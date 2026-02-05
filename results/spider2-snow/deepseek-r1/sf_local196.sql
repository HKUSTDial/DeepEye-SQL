WITH "first_payment_with_rating" AS (
    SELECT 
        p."customer_id",
        TO_TIMESTAMP(p."payment_date") AS "payment_date",
        p."rental_id",
        f."rating"
    FROM "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT" p
    JOIN "SQLITE_SAKILA"."SQLITE_SAKILA"."RENTAL" r ON p."rental_id" = r."rental_id"
    JOIN "SQLITE_SAKILA"."SQLITE_SAKILA"."INVENTORY" i ON r."inventory_id" = i."inventory_id"
    JOIN "SQLITE_SAKILA"."SQLITE_SAKILA"."FILM" f ON i."film_id" = f."film_id"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p."customer_id" ORDER BY TO_TIMESTAMP(p."payment_date") ASC) = 1
),
"customer_totals" AS (
    SELECT 
        "customer_id",
        SUM("amount") AS "total_amount",
        COUNT(DISTINCT "rental_id") AS "total_rentals"
    FROM "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT"
    GROUP BY "customer_id"
)
SELECT 
    fp."rating",
    AVG(ct."total_amount") AS "average_total_amount_spent",
    AVG(ct."total_rentals" - 1) AS "average_subsequent_rentals"
FROM "first_payment_with_rating" fp
JOIN "customer_totals" ct ON fp."customer_id" = ct."customer_id"
GROUP BY fp."rating"