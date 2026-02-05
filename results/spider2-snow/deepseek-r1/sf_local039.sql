WITH filtered_cities AS (
  SELECT "city_id" 
  FROM "PAGILA"."PAGILA"."CITY" 
  WHERE "city" LIKE 'A%' OR "city" LIKE '%-%'
),
customer_rentals AS (
  SELECT r."rental_id", r."inventory_id", 
         DATEDIFF('hour', TRY_TO_TIMESTAMP(r."rental_date"), TRY_TO_TIMESTAMP(r."return_date")) as rental_hours
  FROM "PAGILA"."PAGILA"."RENTAL" r
  JOIN "PAGILA"."PAGILA"."CUSTOMER" c ON r."customer_id" = c."customer_id"
  JOIN "PAGILA"."PAGILA"."ADDRESS" a ON c."address_id" = a."address_id"
  WHERE a."city_id" IN (SELECT "city_id" FROM filtered_cities)
    AND r."return_date" IS NOT NULL
    AND r."rental_date" IS NOT NULL
    AND r."return_date" != ''
    AND r."rental_date" != ''
    AND TRY_TO_TIMESTAMP(r."rental_date") IS NOT NULL
    AND TRY_TO_TIMESTAMP(r."return_date") IS NOT NULL
),
category_totals AS (
  SELECT cat."name" as category_name, 
         SUM(cr.rental_hours) as total_rental_hours
  FROM customer_rentals cr
  JOIN "PAGILA"."PAGILA"."INVENTORY" i ON cr."inventory_id" = i."inventory_id"
  JOIN "PAGILA"."PAGILA"."FILM_CATEGORY" fc ON i."film_id" = fc."film_id"
  JOIN "PAGILA"."PAGILA"."CATEGORY" cat ON fc."category_id" = cat."category_id"
  GROUP BY cat."name"
)
SELECT category_name, total_rental_hours
FROM category_totals
ORDER BY total_rental_hours DESC
LIMIT 1