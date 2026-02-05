WITH first_trigger AS (
  SELECT 
    `session`,
    `stamp` AS trigger_stamp,
    `path` AS trigger_path,
    `search_type` AS trigger_search_type
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY `session` ORDER BY `stamp`) AS rn
    FROM `activity_log`
    WHERE `path` IN ('/detail/', '/complete/')
  )
  WHERE rn = 1
),
pre_counts AS (
  SELECT 
    ft.`session`,
    ft.`trigger_path`,
    ft.`trigger_search_type`,
    COUNT(a.`session`) AS pre_count
  FROM first_trigger ft
  LEFT JOIN `activity_log` a 
    ON a.`session` = ft.`session` 
    AND a.`stamp` < ft.`trigger_stamp`
    AND a.`search_type` != ''
  GROUP BY ft.`session`, ft.`trigger_path`, ft.`trigger_search_type`
),
min_count AS (
  SELECT MIN(`pre_count`) AS min_count
  FROM pre_counts
)
SELECT 
  pc.`session`,
  pc.`trigger_path` AS `path`,
  pc.`trigger_search_type` AS `search_type`
FROM pre_counts pc
INNER JOIN min_count mc ON pc.`pre_count` = mc.`min_count`