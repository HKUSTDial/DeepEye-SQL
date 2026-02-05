SELECT
    "ACTOR"."first_name",
    "ACTOR"."last_name",
    COUNT(*) AS film_count
FROM
    "PAGILA"."PAGILA"."FILM" AS "FILM"
    INNER JOIN "PAGILA"."PAGILA"."LANGUAGE" AS "LANGUAGE"
        ON "FILM"."language_id" = "LANGUAGE"."language_id"
    INNER JOIN "PAGILA"."PAGILA"."FILM_CATEGORY" AS "FILM_CATEGORY"
        ON "FILM"."film_id" = "FILM_CATEGORY"."film_id"
    INNER JOIN "PAGILA"."PAGILA"."CATEGORY" AS "CATEGORY"
        ON "FILM_CATEGORY"."category_id" = "CATEGORY"."category_id"
    INNER JOIN "PAGILA"."PAGILA"."FILM_ACTOR" AS "FILM_ACTOR"
        ON "FILM"."film_id" = "FILM_ACTOR"."film_id"
    INNER JOIN "PAGILA"."PAGILA"."ACTOR" AS "ACTOR"
        ON "FILM_ACTOR"."actor_id" = "ACTOR"."actor_id"
WHERE
    "LANGUAGE"."name" = 'English'
    AND "CATEGORY"."name" = 'Children'
    AND "FILM"."rating" IN ('G', 'PG')
    AND "FILM"."length" <= 120
    AND TRY_TO_NUMBER("FILM"."release_year") BETWEEN 2000 AND 2010
GROUP BY
    "ACTOR"."actor_id",
    "ACTOR"."first_name",
    "ACTOR"."last_name"
ORDER BY
    film_count DESC
LIMIT 1