WITH country_with_nine_days AS (
  SELECT `country_code_2`
  FROM `cities`
  WHERE `insert_date` BETWEEN '2022-01-01' AND '2022-01-31'
  GROUP BY `country_code_2`
  HAVING COUNT(DISTINCT `insert_date`) = 9
  ORDER BY `country_code_2`
  LIMIT 1
),
distinct_dates AS (
  SELECT DISTINCT `insert_date`
  FROM `cities`
  WHERE `country_code_2` = (SELECT `country_code_2` FROM country_with_nine_days)
    AND `insert_date` BETWEEN '2022-01-01' AND '2022-01-31'
  ORDER BY `insert_date`
),
date_sequence AS (
  SELECT `insert_date`,
         julianday(`insert_date`) - ROW_NUMBER() OVER (ORDER BY `insert_date`) AS grp
  FROM distinct_dates
),
streaks AS (
  SELECT grp,
         MIN(`insert_date`) AS streak_start,
         MAX(`insert_date`) AS streak_end,
         COUNT(*) AS streak_length
  FROM date_sequence
  GROUP BY grp
),
longest_streak AS (
  SELECT streak_start, streak_end, streak_length
  FROM streaks
  ORDER BY streak_length DESC, streak_start
  LIMIT 1
),
total_and_capital AS (
  SELECT 
    (SELECT COUNT(*)
     FROM `cities`
     WHERE `country_code_2` = (SELECT `country_code_2` FROM country_with_nine_days)
       AND `insert_date` BETWEEN (SELECT streak_start FROM longest_streak)
                             AND (SELECT streak_end FROM longest_streak)
    ) AS total_entries,
    (SELECT COUNT(*)
     FROM `cities`
     WHERE `country_code_2` = (SELECT `country_code_2` FROM country_with_nine_days)
       AND `insert_date` BETWEEN (SELECT streak_start FROM longest_streak)
                             AND (SELECT streak_end FROM longest_streak)
       AND `capital` = 1
    ) AS capital_entries
)
SELECT 
  (SELECT `country_name`
   FROM `cities_countries`
   WHERE `country_code_2` = (SELECT `country_code_2` FROM country_with_nine_days)
  ) AS country_name,
  capital_entries * 1.0 / total_entries AS proportion
FROM total_and_capital;