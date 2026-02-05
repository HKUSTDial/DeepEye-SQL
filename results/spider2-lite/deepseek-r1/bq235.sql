WITH outpatient_agg AS (
  SELECT 
    `provider_id`,
    ANY_VALUE(`provider_name`) AS `provider_name`,
    SUM(`average_total_payments` * `outpatient_services`) AS `out_total_payments`,
    SUM(`outpatient_services`) AS `out_total_services`
  FROM `bigquery-public-data.cms_medicare.outpatient_charges_2014`
  GROUP BY `provider_id`
),
inpatient_agg AS (
  SELECT 
    `provider_id`,
    SUM(`average_total_payments` * `total_discharges`) AS `in_total_payments`,
    SUM(`total_discharges`) AS `in_total_discharges`
  FROM `bigquery-public-data.cms_medicare.inpatient_charges_2014`
  GROUP BY `provider_id`
),
combined AS (
  SELECT 
    o.`provider_name`,
    (o.`out_total_payments` + i.`in_total_payments`) / (o.`out_total_services` + i.`in_total_discharges`) AS `combined_avg_cost`
  FROM outpatient_agg o
  JOIN inpatient_agg i ON o.`provider_id` = i.`provider_id`
  WHERE (o.`out_total_services` + i.`in_total_discharges`) > 0
)
SELECT `provider_name`
FROM combined
ORDER BY `combined_avg_cost` DESC
LIMIT 1