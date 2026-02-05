WITH `landing` AS (
  SELECT 
    `session`,
    `path`,
    ROW_NUMBER() OVER (PARTITION BY `session` ORDER BY `stamp`) AS `rn`
  FROM `activity_log`
),
`exit` AS (
  SELECT 
    `session`,
    `path`,
    ROW_NUMBER() OVER (PARTITION BY `session` ORDER BY `stamp` DESC) AS `rn`
  FROM `activity_log`
),
`combined` AS (
  SELECT `session`, `path` FROM `landing` WHERE `rn` = 1
  UNION
  SELECT `session`, `path` FROM `exit` WHERE `rn` = 1
)
SELECT 
  `path`,
  COUNT(DISTINCT `session`) AS `unique_sessions_count`
FROM `combined`
GROUP BY `path`
ORDER BY `path`