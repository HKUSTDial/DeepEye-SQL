WITH first_purchase AS (
  SELECT `customer_id`, MIN(`payment_date`) AS `first_purchase_date`
  FROM `payment`
  GROUP BY `customer_id`
),
customer_totals AS (
  SELECT
    p.`customer_id`,
    f.`first_purchase_date`,
    SUM(p.`amount`) AS `total_ltv`,
    SUM(CASE WHEN p.`payment_date` <= datetime(f.`first_purchase_date`, '+7 days') THEN p.`amount` ELSE 0 END) AS `sum_7_days`,
    SUM(CASE WHEN p.`payment_date` <= datetime(f.`first_purchase_date`, '+30 days') THEN p.`amount` ELSE 0 END) AS `sum_30_days`
  FROM `payment` p
  INNER JOIN `first_purchase` f ON p.`customer_id` = f.`customer_id`
  GROUP BY p.`customer_id`, f.`first_purchase_date`
  HAVING SUM(p.`amount`) > 0
)
SELECT
  AVG(100.0 * `sum_7_days` / `total_ltv`) AS `avg_pct_7_days`,
  AVG(100.0 * `sum_30_days` / `total_ltv`) AS `avg_pct_30_days`,
  AVG(`total_ltv`) AS `avg_ltv`
FROM `customer_totals`