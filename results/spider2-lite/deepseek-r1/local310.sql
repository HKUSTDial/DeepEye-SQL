WITH driver_totals AS (
  SELECT 
    r.`year`,
    res.`driver_id`,
    SUM(res.`points`) AS driver_total
  FROM `results` res
  JOIN `races` r ON res.`race_id` = r.`race_id`
  GROUP BY r.`year`, res.`driver_id`
),
max_driver AS (
  SELECT 
    `year`,
    MAX(`driver_total`) AS max_driver_total
  FROM driver_totals
  GROUP BY `year`
),
constructor_totals AS (
  SELECT 
    r.`year`,
    res.`constructor_id`,
    SUM(res.`points`) AS constructor_total
  FROM `results` res
  JOIN `races` r ON res.`race_id` = r.`race_id`
  GROUP BY r.`year`, res.`constructor_id`
),
max_constructor AS (
  SELECT 
    `year`,
    MAX(`constructor_total`) AS max_constructor_total
  FROM constructor_totals
  GROUP BY `year`
)
SELECT 
  md.`year`
FROM max_driver md
JOIN max_constructor mc ON md.`year` = mc.`year`
ORDER BY (md.`max_driver_total` + mc.`max_constructor_total`) ASC, md.`year` ASC
LIMIT 3