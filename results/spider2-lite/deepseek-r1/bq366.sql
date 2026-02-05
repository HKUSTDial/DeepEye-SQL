WITH `global_label_counts` AS (
  SELECT 
    `l`.`description` AS `label`,
    COUNT(DISTINCT `o`.`object_id`) AS `total_artwork_count`
  FROM `bigquery-public-data.the_met.objects` AS `o`
  JOIN `bigquery-public-data.the_met.vision_api_data` AS `v`
    ON `o`.`object_id` = `v`.`object_id`
  CROSS JOIN UNNEST(`v`.`labelAnnotations`) AS `l`
  WHERE `l`.`description` IS NOT NULL
  GROUP BY `l`.`description`
  HAVING COUNT(DISTINCT `o`.`object_id`) >= 500
),
`per_period_label_counts` AS (
  SELECT 
    `o`.`period`,
    `l`.`description` AS `label`,
    COUNT(DISTINCT `o`.`object_id`) AS `period_artwork_count`
  FROM `bigquery-public-data.the_met.objects` AS `o`
  JOIN `bigquery-public-data.the_met.vision_api_data` AS `v`
    ON `o`.`object_id` = `v`.`object_id`
  CROSS JOIN UNNEST(`v`.`labelAnnotations`) AS `l`
  INNER JOIN `global_label_counts` `glc`
    ON `glc`.`label` = `l`.`description`
  WHERE `o`.`period` IS NOT NULL
    AND `l`.`description` IS NOT NULL
  GROUP BY `o`.`period`, `l`.`description`
),
`ranked_labels` AS (
  SELECT 
    `period`,
    `label`,
    `period_artwork_count`,
    ROW_NUMBER() OVER (PARTITION BY `period` ORDER BY `period_artwork_count` DESC) AS `rn`
  FROM `per_period_label_counts`
)
SELECT 
  `period`,
  `label`,
  `period_artwork_count` AS `associated_count`
FROM `ranked_labels`
WHERE `rn` <= 3
ORDER BY `period`, `rn`