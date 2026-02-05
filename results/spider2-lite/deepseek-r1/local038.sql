WITH eligible_films AS (
  SELECT f.`film_id`
  FROM `film` f
  JOIN `language` l ON f.`language_id` = l.`language_id`
  JOIN `film_category` fc ON f.`film_id` = fc.`film_id`
  JOIN `category` c ON fc.`category_id` = c.`category_id`
  WHERE l.`name` = 'English'
    AND c.`name` = 'Children'
    AND f.`rating` IN ('G', 'PG')
    AND f.`length` <= 120
    AND f.`release_year` BETWEEN '2000' AND '2010'
),
actor_counts AS (
  SELECT a.`actor_id`, a.`first_name`, a.`last_name`, COUNT(*) AS `film_count`,
         RANK() OVER (ORDER BY COUNT(*) DESC) AS `rnk`
  FROM `actor` a
  JOIN `film_actor` fa ON a.`actor_id` = fa.`actor_id`
  JOIN eligible_films ef ON fa.`film_id` = ef.`film_id`
  GROUP BY a.`actor_id`, a.`first_name`, a.`last_name`
)
SELECT `first_name` || ' ' || `last_name` AS `full_name`
FROM actor_counts
WHERE `rnk` = 1