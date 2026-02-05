WITH film_revenue AS (
    SELECT 
        i.film_id,
        SUM(p.amount) AS total_revenue
    FROM payment p
    JOIN rental r ON p.rental_id = r.rental_id
    JOIN inventory i ON r.inventory_id = i.inventory_id
    GROUP BY i.film_id
),
film_actor_count AS (
    SELECT 
        film_id,
        COUNT(actor_id) AS actor_count
    FROM film_actor
    GROUP BY film_id
),
film_revenue_per_actor AS (
    SELECT 
        fr.film_id,
        fr.total_revenue,
        fac.actor_count,
        fr.total_revenue * 1.0 / fac.actor_count AS revenue_per_actor
    FROM film_revenue fr
    JOIN film_actor_count fac ON fr.film_id = fac.film_id
),
actor_film_revenue AS (
    SELECT 
        a.actor_id,
        a.first_name,
        a.last_name,
        f.film_id,
        f.title,
        frpa.revenue_per_actor
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film f ON fa.film_id = f.film_id
    JOIN film_revenue_per_actor frpa ON f.film_id = frpa.film_id
),
ranked_films AS (
    SELECT 
        actor_id,
        first_name,
        last_name,
        film_id,
        title,
        revenue_per_actor,
        ROW_NUMBER() OVER (PARTITION BY actor_id ORDER BY revenue_per_actor DESC) AS rank
    FROM actor_film_revenue
),
top_three AS (
    SELECT 
        actor_id,
        first_name,
        last_name,
        film_id,
        title,
        revenue_per_actor,
        rank
    FROM ranked_films
    WHERE rank <= 3
),
actor_avg AS (
    SELECT 
        actor_id,
        AVG(revenue_per_actor) AS avg_revenue_per_actor
    FROM top_three
    GROUP BY actor_id
)
SELECT 
    t.actor_id,
    t.first_name,
    t.last_name,
    t.film_id,
    t.title,
    t.revenue_per_actor,
    a.avg_revenue_per_actor
FROM top_three t
JOIN actor_avg a ON t.actor_id = a.actor_id
ORDER BY t.actor_id, t.rank;