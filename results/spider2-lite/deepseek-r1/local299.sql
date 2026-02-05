WITH daily_net AS (
  SELECT `customer_id`, `txn_date`, 
         SUM(CASE WHEN `txn_type` = 'deposit' THEN `txn_amount` ELSE -`txn_amount` END) AS daily_net
  FROM `customer_transactions`
  GROUP BY `customer_id`, `txn_date`
),
customer_ranges AS (
  SELECT `customer_id`, MIN(`txn_date`) AS start_date, MAX(`txn_date`) AS end_date
  FROM `customer_transactions`
  GROUP BY `customer_id`
),
all_dates AS (
  SELECT MIN(`txn_date`) AS date FROM `customer_transactions`
  UNION ALL
  SELECT date(date, '+1 day') FROM all_dates 
  WHERE date < (SELECT MAX(`txn_date`) FROM `customer_transactions`)
),
customer_dates AS (
  SELECT cr.`customer_id`, ad.date
  FROM all_dates ad
  JOIN customer_ranges cr ON ad.date BETWEEN cr.start_date AND cr.end_date
),
daily_balances AS (
  SELECT cd.`customer_id`, cd.date,
         COALESCE(dn.daily_net, 0) AS daily_net,
         SUM(COALESCE(dn.daily_net, 0)) OVER (PARTITION BY cd.`customer_id` ORDER BY cd.date ROWS UNBOUNDED PRECEDING) AS running_balance
  FROM customer_dates cd
  LEFT JOIN daily_net dn ON cd.`customer_id` = dn.`customer_id` AND cd.date = dn.`txn_date`
),
rolling_avg AS (
  SELECT `customer_id`, date, running_balance,
         CASE 
           WHEN COUNT(running_balance) OVER (PARTITION BY `customer_id` ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 30 
           THEN MAX(0, AVG(running_balance) OVER (PARTITION BY `customer_id` ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))
           ELSE NULL 
         END AS rolling_avg_30d
  FROM daily_balances
),
monthly_data AS (
  SELECT `customer_id`, 
         strftime('%Y-%m', date) AS month,
         MAX(rolling_avg_30d) AS max_rolling_avg
  FROM rolling_avg
  WHERE rolling_avg_30d IS NOT NULL
  GROUP BY `customer_id`, strftime('%Y-%m', date)
),
first_month_per_customer AS (
  SELECT `customer_id`, MIN(strftime('%Y-%m', `txn_date`)) AS first_month
  FROM `customer_transactions`
  GROUP BY `customer_id`
),
final_data AS (
  SELECT md.`customer_id`, md.month, md.max_rolling_avg
  FROM monthly_data md
  LEFT JOIN first_month_per_customer fm ON md.`customer_id` = fm.`customer_id`
  WHERE md.month != fm.first_month OR fm.first_month IS NULL
)
SELECT month, SUM(max_rolling_avg) AS total_sum_max_rolling_avg
FROM final_data
GROUP BY month
ORDER BY month;