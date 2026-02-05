WITH monthly_totals AS (
    SELECT customer_id, 
           strftime('%Y-%m', `payment_date`) AS month,
           SUM(`amount`) AS monthly_total
    FROM `payment`
    GROUP BY customer_id, strftime('%Y-%m', `payment_date`)
),
monthly_changes AS (
    SELECT customer_id,
           monthly_total - LAG(monthly_total) OVER (PARTITION BY customer_id ORDER BY month) AS monthly_change
    FROM monthly_totals
),
avg_changes AS (
    SELECT customer_id, 
           AVG(monthly_change) AS avg_monthly_change
    FROM monthly_changes
    WHERE monthly_change IS NOT NULL
    GROUP BY customer_id
)
SELECT `customer`.`first_name`, `customer`.`last_name`
FROM avg_changes
JOIN `customer` ON avg_changes.customer_id = `customer`.`customer_id`
ORDER BY avg_monthly_change DESC
LIMIT 1