WITH `high_rated_movies` AS (
    SELECT `movie_id`
    FROM `ratings`
    WHERE `avg_rating` > 8
),
`top_genres` AS (
    SELECT `genre`
    FROM `genre`
    WHERE `movie_id` IN (SELECT `movie_id` FROM `high_rated_movies`)
    GROUP BY `genre`
    ORDER BY COUNT(DISTINCT `movie_id`) DESC
    LIMIT 3
),
`movies_in_top_genres` AS (
    SELECT DISTINCT `g`.`movie_id`
    FROM `genre` AS `g`
    WHERE `g`.`genre` IN (SELECT `genre` FROM `top_genres`)
      AND `g`.`movie_id` IN (SELECT `movie_id` FROM `high_rated_movies`)
),
`director_counts` AS (
    SELECT `dm`.`name_id`, COUNT(DISTINCT `dm`.`movie_id`) AS `movie_count`
    FROM `director_mapping` AS `dm`
    WHERE `dm`.`movie_id` IN (SELECT `movie_id` FROM `movies_in_top_genres`)
    GROUP BY `dm`.`name_id`
    ORDER BY `movie_count` DESC
    LIMIT 4
)
SELECT `n`.`name`, `dc`.`movie_count`
FROM `director_counts` AS `dc`
JOIN `names` AS `n` ON `dc`.`name_id` = `n`.`id`
ORDER BY `dc`.`movie_count` DESC, `n`.`name`