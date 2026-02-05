WITH shahrukh AS (
  SELECT "PID" FROM "DB_IMDB"."DB_IMDB"."PERSON" WHERE TRIM("Name") = 'Shahrukh Khan'
),
shahrukh_movies AS (
  SELECT DISTINCT "MID" FROM "DB_IMDB"."DB_IMDB"."M_CAST" WHERE TRIM("PID") IN (SELECT "PID" FROM shahrukh)
),
direct_coactors AS (
  SELECT DISTINCT TRIM("PID") AS "PID" FROM "DB_IMDB"."DB_IMDB"."M_CAST" WHERE "MID" IN (SELECT "MID" FROM shahrukh_movies) AND TRIM("PID") NOT IN (SELECT "PID" FROM shahrukh)
),
coactor_movies AS (
  SELECT DISTINCT "MID" FROM "DB_IMDB"."DB_IMDB"."M_CAST" WHERE TRIM("PID") IN (SELECT "PID" FROM direct_coactors)
),
distance2_candidates AS (
  SELECT DISTINCT TRIM("PID") AS "PID" FROM "DB_IMDB"."DB_IMDB"."M_CAST" WHERE "MID" IN (SELECT "MID" FROM coactor_movies)
),
actors_distance2 AS (
  SELECT "PID" FROM distance2_candidates WHERE "PID" NOT IN (SELECT "PID" FROM shahrukh) AND "PID" NOT IN (SELECT "PID" FROM direct_coactors)
)
SELECT COUNT(*) AS "count" FROM actors_distance2