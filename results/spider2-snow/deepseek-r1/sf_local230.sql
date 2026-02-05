WITH high_rated_movies AS (
  SELECT "movie_id"
  FROM "IMDB_MOVIES"."IMDB_MOVIES"."RATINGS"
  WHERE "avg_rating" > 8
),
movie_genres AS (
  SELECT DISTINCT hrm."movie_id", g."genre"
  FROM high_rated_movies hrm
  JOIN "IMDB_MOVIES"."IMDB_MOVIES"."GENRE" g
    ON hrm."movie_id" = g."movie_id"
),
genre_counts AS (
  SELECT "genre", COUNT(DISTINCT "movie_id") AS movie_count
  FROM movie_genres
  GROUP BY "genre"
  ORDER BY movie_count DESC
  LIMIT 3
),
top_genre_movies AS (
  SELECT DISTINCT mg."movie_id", mg."genre"
  FROM movie_genres mg
  WHERE mg."genre" IN (SELECT "genre" FROM genre_counts)
),
directors AS (
  SELECT DISTINCT tgm."movie_id", dm."name_id", n."name"
  FROM top_genre_movies tgm
  JOIN "IMDB_MOVIES"."IMDB_MOVIES"."DIRECTOR_MAPPING" dm
    ON tgm."movie_id" = dm."movie_id"
  JOIN "IMDB_MOVIES"."IMDB_MOVIES"."NAMES" n
    ON dm."name_id" = n."id"
),
director_counts AS (
  SELECT "name_id", "name", COUNT(DISTINCT "movie_id") AS movie_count
  FROM directors
  GROUP BY "name_id", "name"
  ORDER BY movie_count DESC
  LIMIT 4
)
SELECT "name", movie_count
FROM director_counts
ORDER BY movie_count DESC