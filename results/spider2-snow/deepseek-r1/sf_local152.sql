SELECT 
  n."id" AS director_id,
  n."name" AS director_name,
  movie_counts.movie_count,
  ROUND(movie_counts.avg_duration) AS avg_inter_movie_duration,
  ROUND(rating_stats.avg_rating_raw, 2) AS avg_rating,
  rating_stats.total_votes,
  rating_stats.min_rating,
  rating_stats.max_rating,
  movie_counts.total_duration AS total_movie_duration
FROM (
  SELECT 
    dm."name_id" AS name_id,
    COUNT(DISTINCT dm."movie_id") as movie_count,
    AVG(m."duration") as avg_duration,
    SUM(m."duration") as total_duration
  FROM "IMDB_MOVIES"."IMDB_MOVIES"."DIRECTOR_MAPPING" dm
  JOIN "IMDB_MOVIES"."IMDB_MOVIES"."MOVIES" m ON dm."movie_id" = m."id"
  GROUP BY dm."name_id"
) movie_counts
JOIN (
  SELECT 
    dm."name_id" AS name_id,
    AVG(r."avg_rating") as avg_rating_raw,
    SUM(r."total_votes") as total_votes,
    MIN(r."avg_rating") as min_rating,
    MAX(r."avg_rating") as max_rating
  FROM "IMDB_MOVIES"."IMDB_MOVIES"."DIRECTOR_MAPPING" dm
  JOIN "IMDB_MOVIES"."IMDB_MOVIES"."RATINGS" r ON dm."movie_id" = r."movie_id"
  GROUP BY dm."name_id"
) rating_stats ON movie_counts.name_id = rating_stats.name_id
JOIN "IMDB_MOVIES"."IMDB_MOVIES"."NAMES" n ON movie_counts.name_id = n."id"
ORDER BY movie_counts.movie_count DESC, movie_counts.total_duration DESC
LIMIT 9