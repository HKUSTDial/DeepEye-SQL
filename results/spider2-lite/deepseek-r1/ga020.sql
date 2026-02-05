WITH cohort AS (
  SELECT
    `user_pseudo_id`,
    MIN(PARSE_DATE('%Y%m%d', `event_date`)) AS `first_engagement_date`
  FROM `firebase-public-project.analytics_153293282`.`events_*`
  WHERE `event_name` = 'session_start'
    AND _TABLE_SUFFIX BETWEEN '20180601' AND '20180815'
  GROUP BY `user_pseudo_id`
  HAVING MIN(PARSE_DATE('%Y%m%d', `event_date`)) BETWEEN '2018-08-01' AND '2018-08-15'
),
first_quickplay AS (
  SELECT
    c.`user_pseudo_id`,
    c.`first_engagement_date`,
    e.`event_name` AS `quickplay_event_type`,
    ROW_NUMBER() OVER (PARTITION BY c.`user_pseudo_id` ORDER BY e.`event_timestamp`) AS `rn`
  FROM `cohort` c
  JOIN `firebase-public-project.analytics_153293282`.`events_*` e
    ON c.`user_pseudo_id` = e.`user_pseudo_id`
    AND e.`event_date` = FORMAT_DATE('%Y%m%d', c.`first_engagement_date`)
  WHERE e.`event_name` != 'session_start'
    AND _TABLE_SUFFIX BETWEEN '20180801' AND '20180815'
),
user_quickplay AS (
  SELECT
    `user_pseudo_id`,
    `first_engagement_date`,
    `quickplay_event_type`
  FROM `first_quickplay`
  WHERE `rn` = 1
),
retained_users AS (
  SELECT DISTINCT
    e.`user_pseudo_id`
  FROM `firebase-public-project.analytics_153293282`.`events_*` e
  JOIN `cohort` c
    ON e.`user_pseudo_id` = c.`user_pseudo_id`
  WHERE e.`event_name` = 'session_start'
    AND _TABLE_SUFFIX BETWEEN '20180801' AND '20180829'
    AND PARSE_DATE('%Y%m%d', e.`event_date`) BETWEEN DATE_ADD(c.`first_engagement_date`, INTERVAL 8 DAY) AND DATE_ADD(c.`first_engagement_date`, INTERVAL 14 DAY)
)
SELECT
  u.`quickplay_event_type`,
  COUNT(*) AS `total_users`,
  COUNT(DISTINCT r.`user_pseudo_id`) AS `retained_users`,
  SAFE_DIVIDE(COUNT(DISTINCT r.`user_pseudo_id`), COUNT(*)) AS `retention_rate`
FROM `user_quickplay` u
LEFT JOIN `retained_users` r
  ON u.`user_pseudo_id` = r.`user_pseudo_id`
GROUP BY u.`quickplay_event_type`
ORDER BY `retention_rate` ASC
LIMIT 1