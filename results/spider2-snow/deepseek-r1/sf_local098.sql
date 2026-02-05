WITH actor_years AS (
    SELECT DISTINCT
        TRIM(c."PID") AS actor_id,
        TRY_CAST(m."year" AS INTEGER) AS movie_year
    FROM "DB_IMDB"."DB_IMDB"."M_CAST" c
    INNER JOIN "DB_IMDB"."DB_IMDB"."MOVIE" m ON c."MID" = m."MID"
    WHERE TRY_CAST(m."year" AS INTEGER) IS NOT NULL
),
actor_gaps AS (
    SELECT
        actor_id,
        movie_year,
        LEAD(movie_year) OVER (PARTITION BY actor_id ORDER BY movie_year) AS next_year
    FROM actor_years
),
actor_has_bad_gap AS (
    SELECT
        actor_id,
        MAX(CASE WHEN next_year - movie_year > 4 THEN 1 ELSE 0 END) AS has_bad_gap
    FROM actor_gaps
    GROUP BY actor_id
)
SELECT COUNT(*) AS actor_count
FROM actor_has_bad_gap
WHERE has_bad_gap = 0