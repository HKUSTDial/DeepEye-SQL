WITH home_hits AS (
  SELECT
    `fullVisitorId`,
    `visitId`,
    `hits`.`hitNumber`,
    `hits`.`time` AS current_time,
    `hits`.`page`.`pagePath` AS current_page,
    LEAD(`hits`.`page`.`pagePath`) OVER (
      PARTITION BY `fullVisitorId`, `visitId`
      ORDER BY `hits`.`hitNumber`
    ) AS next_page,
    LEAD(`hits`.`time`) OVER (
      PARTITION BY `fullVisitorId`, `visitId`
      ORDER BY `hits`.`hitNumber`
    ) AS next_time
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_201701*`,
    UNNEST(`hits`) AS `hits`
  WHERE
    _TABLE_SUFFIX BETWEEN '01' AND '31'
    AND CONTAINS_SUBSTR(`trafficSource`.`campaign`, 'Data Share')
    AND STARTS_WITH(`hits`.`page`.`pagePath`, '/home')
    AND `hits`.`type` = 'PAGE'
),
time_calculations AS (
  SELECT
    `next_page`,
    (`next_time` - `current_time`) / 1000.0 AS time_on_home_seconds
  FROM
    `home_hits`
  WHERE
    `next_page` IS NOT NULL
    AND `next_time` IS NOT NULL
    AND `current_time` IS NOT NULL
)
SELECT
  `next_page` AS most_common_next_page,
  COUNT(*) AS visit_count,
  MAX(`time_on_home_seconds`) AS max_time_on_home_seconds
FROM
  `time_calculations`
GROUP BY
  `next_page`
ORDER BY
  `visit_count` DESC
LIMIT 1