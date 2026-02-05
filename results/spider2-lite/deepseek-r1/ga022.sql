WITH cohort AS (
  SELECT 
    `user_pseudo_id`,
    MIN(DATETIME(TIMESTAMP_MICROS(`event_timestamp`), 'Asia/Shanghai')) AS `first_open_shanghai_datetime`
  FROM `firebase-public-project`.`analytics_153293282`.`events_*`
  WHERE `event_name` = 'first_open'
    AND `_TABLE_SUFFIX` BETWEEN '20180831' AND '20180930'
  GROUP BY `user_pseudo_id`
  HAVING DATE(MIN(DATETIME(TIMESTAMP_MICROS(`event_timestamp`), 'Asia/Shanghai'))) BETWEEN '2018-09-01' AND '2018-09-07'
),
user_retention AS (
  SELECT 
    c.`user_pseudo_id`,
    c.`first_open_shanghai_datetime`,
    MAX(CASE WHEN DATE(DATETIME(TIMESTAMP_MICROS(e.`event_timestamp`), 'Asia/Shanghai')) BETWEEN DATE_ADD(DATE(c.`first_open_shanghai_datetime`), INTERVAL 1 DAY) AND DATE_ADD(DATE(c.`first_open_shanghai_datetime`), INTERVAL 7 DAY) THEN 1 ELSE 0 END) AS `retained_week1`,
    MAX(CASE WHEN DATE(DATETIME(TIMESTAMP_MICROS(e.`event_timestamp`), 'Asia/Shanghai')) BETWEEN DATE_ADD(DATE(c.`first_open_shanghai_datetime`), INTERVAL 8 DAY) AND DATE_ADD(DATE(c.`first_open_shanghai_datetime`), INTERVAL 14 DAY) THEN 1 ELSE 0 END) AS `retained_week2`,
    MAX(CASE WHEN DATE(DATETIME(TIMESTAMP_MICROS(e.`event_timestamp`), 'Asia/Shanghai')) BETWEEN DATE_ADD(DATE(c.`first_open_shanghai_datetime`), INTERVAL 15 DAY) AND DATE_ADD(DATE(c.`first_open_shanghai_datetime`), INTERVAL 21 DAY) THEN 1 ELSE 0 END) AS `retained_week3`
  FROM cohort c
  LEFT JOIN `firebase-public-project`.`analytics_153293282`.`events_*` e
    ON c.`user_pseudo_id` = e.`user_pseudo_id`
    AND `_TABLE_SUFFIX` BETWEEN '20180901' AND '20180930'
  GROUP BY c.`user_pseudo_id`, c.`first_open_shanghai_datetime`
)
SELECT 
  ROUND(SUM(`retained_week1`) * 100.0 / COUNT(*), 2) AS `week1_retention_rate`,
  ROUND(SUM(`retained_week2`) * 100.0 / COUNT(*), 2) AS `week2_retention_rate`,
  ROUND(SUM(`retained_week3`) * 100.0 / COUNT(*), 2) AS `week3_retention_rate`
FROM user_retention