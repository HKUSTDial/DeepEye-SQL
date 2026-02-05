WITH film_revenue AS (
    SELECT 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."INVENTORY"."film_id",
        SUM("SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT"."amount") AS total_film_revenue
    FROM 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT"
    JOIN 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."RENTAL" ON "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT"."rental_id" = "SQLITE_SAKILA"."SQLITE_SAKILA"."RENTAL"."rental_id"
    JOIN 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."INVENTORY" ON "SQLITE_SAKILA"."SQLITE_SAKILA"."RENTAL"."inventory_id" = "SQLITE_SAKILA"."SQLITE_SAKILA"."INVENTORY"."inventory_id"
    GROUP BY 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."INVENTORY"."film_id"
),
actors_per_film AS (
    SELECT 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."FILM_ACTOR"."film_id",
        COUNT(DISTINCT "SQLITE_SAKILA"."SQLITE_SAKILA"."FILM_ACTOR"."actor_id") AS actor_count
    FROM 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."FILM_ACTOR"
    GROUP BY 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."FILM_ACTOR"."film_id"
),
revenue_per_actor_per_film AS (
    SELECT 
        fr."film_id",
        fr.total_film_revenue,
        apf.actor_count,
        fr.total_film_revenue / apf.actor_count AS revenue_per_actor
    FROM 
        film_revenue fr
    JOIN 
        actors_per_film apf ON fr."film_id" = apf."film_id"
),
ranked_films_per_actor AS (
    SELECT 
        a."actor_id",
        a."first_name",
        a."last_name",
        f."film_id",
        f."title",
        rp.revenue_per_actor AS "revenue_per_actor",
        ROW_NUMBER() OVER (PARTITION BY a."actor_id" ORDER BY rp.revenue_per_actor DESC) AS "rank"
    FROM 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."ACTOR" a
    JOIN 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."FILM_ACTOR" fa ON a."actor_id" = fa."actor_id"
    JOIN 
        "SQLITE_SAKILA"."SQLITE_SAKILA"."FILM" f ON fa."film_id" = f."film_id"
    JOIN 
        revenue_per_actor_per_film rp ON f."film_id" = rp."film_id"
)
SELECT 
    "actor_id",
    "first_name",
    "last_name",
    "film_id",
    "title",
    "revenue_per_actor",
    AVG("revenue_per_actor") OVER (PARTITION BY "actor_id") AS avg_revenue_per_actor
FROM 
    ranked_films_per_actor
WHERE 
    "rank" <= 3
ORDER BY 
    "actor_id", "rank"