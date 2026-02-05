WITH top_customers AS (
    SELECT p.customer_id
    FROM payment p
    GROUP BY p.customer_id
    ORDER BY SUM(p.amount) DESC
    LIMIT 10
),
monthly_totals AS (
    SELECT p.customer_id,
           CAST(strftime('%Y', p.payment_date) AS INTEGER) AS year,
           CAST(strftime('%m', p.payment_date) AS INTEGER) AS month,
           SUM(p.amount) AS monthly_total
    FROM payment p
    WHERE p.customer_id IN (SELECT customer_id FROM top_customers)
    GROUP BY p.customer_id, year, month
),
monthly_with_number AS (
    SELECT customer_id, year, month, monthly_total,
           year * 12 + month AS month_number
    FROM monthly_totals
),
monthly_lag AS (
    SELECT customer_id, year, month, monthly_total, month_number,
           LAG(monthly_total) OVER (PARTITION BY customer_id ORDER BY month_number) AS prev_month_total,
           LAG(month_number) OVER (PARTITION BY customer_id ORDER BY month_number) AS prev_month_number
    FROM monthly_with_number
),
changes AS (
    SELECT customer_id, year, month,
           (monthly_total - prev_month_total) AS change
    FROM monthly_lag
    WHERE prev_month_number = month_number - 1
),
abs_changes AS (
    SELECT customer_id, year, month, change,
           ABS(change) AS abs_change
    FROM changes
),
max_change AS (
    SELECT customer_id, year, month, change
    FROM abs_changes
    ORDER BY abs_change DESC
    LIMIT 1
)
SELECT c.first_name, c.last_name,
       printf('%04d-%02d', mc.year, mc.month) AS payment_month,
       ROUND(mc.change, 2) AS difference
FROM max_change mc
JOIN customer c ON mc.customer_id = c.customer_id;