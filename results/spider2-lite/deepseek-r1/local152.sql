SELECT
  `names`.`id` AS director_id,
  `names`.`name` AS director_name,
  COUNT(DISTINCT `director_mapping`.`movie_id`) AS movie_count,
  ROUND(AVG(`movies`.`duration`)) AS avg_inter_movie_duration,
  ROUND(AVG(`ratings`.`avg_rating`), 2) AS avg_rating,
  SUM(`ratings`.`total_votes`) AS total_votes,
  MIN(`ratings`.`avg_rating`) AS min_rating,
  MAX(`ratings`.`avg_rating`) AS max_rating,
  SUM(`movies`.`duration`) AS total_movie_duration
FROM `director_mapping`
JOIN `names` ON `director_mapping`.`name_id` = `names`.`id`
JOIN `movies` ON `director_mapping`.`movie_id` = `movies`.`id`
JOIN `ratings` ON `director_mapping`.`movie_id` = `ratings`.`movie_id`
GROUP BY `names`.`id`, `names`.`name`
ORDER BY movie_count DESC, total_movie_duration DESC
LIMIT 9