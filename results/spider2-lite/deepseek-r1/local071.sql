WITH `dates_per_country` AS (
  SELECT DISTINCT `country_code_2`, DATE(`insert_date`) AS `dt`
  FROM `cities`
  WHERE `insert_date` BETWEEN '2022-06-01' AND '2022-06-30'
),
`numbered` AS (
  SELECT `country_code_2`, `dt`,
         ROW_NUMBER() OVER (PARTITION BY `country_code_2` ORDER BY `dt`) AS `rn`
  FROM `dates_per_country`
),
`grouped` AS (
  SELECT `country_code_2`, `dt`, `rn`,
         julianday(`dt`) - `rn` AS `grp`
  FROM `numbered`
),
`streaks` AS (
  SELECT `country_code_2`, `grp`,
         COUNT(*) AS `streak_length`
  FROM `grouped`
  GROUP BY `country_code_2`, `grp`
),
`max_per_country` AS (
  SELECT `country_code_2`, MAX(`streak_length`) AS `longest_streak`
  FROM `streaks`
  GROUP BY `country_code_2`
),
`overall_max` AS (
  SELECT MAX(`longest_streak`) AS `max_streak`
  FROM `max_per_country`
)
SELECT `country_code_2`
FROM `max_per_country`
WHERE `longest_streak` = (SELECT `max_streak` FROM `overall_max`)
ORDER BY `country_code_2`;