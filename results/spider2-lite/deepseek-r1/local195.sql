WITH top_actors AS (
    SELECT actor_id
    FROM film_actor
    GROUP BY actor_id
    ORDER BY COUNT(*) DESC
    LIMIT 5
),
customers_with_top_actors AS (
    SELECT DISTINCT rental.customer_id
    FROM rental
    JOIN inventory ON rental.inventory_id = inventory.inventory_id
    JOIN film_actor ON inventory.film_id = film_actor.film_id
    WHERE film_actor.actor_id IN (SELECT actor_id FROM top_actors)
),
total_customers AS (
    SELECT COUNT(*) AS total
    FROM customer
)
SELECT 
    (SELECT COUNT(*) FROM customers_with_top_actors) * 100.0 / 
    (SELECT total FROM total_customers) AS percentage_customers;