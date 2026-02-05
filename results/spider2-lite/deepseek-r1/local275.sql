WITH months_ranked AS (
    SELECT DISTINCT mth, 
           ROW_NUMBER() OVER (ORDER BY mth) AS month_index
    FROM monthly_sales
),
sales_with_index AS (
    SELECT ms.product_id, ms.mth, ms.qty, mr.month_index
    FROM monthly_sales ms
    JOIN months_ranked mr ON ms.mth = mr.mth
),
with_sums AS (
    SELECT product_id, mth, month_index, qty,
           SUM(qty) OVER (PARTITION BY product_id ORDER BY month_index ROWS BETWEEN 5 PRECEDING AND 6 FOLLOWING) AS sum1,
           SUM(qty) OVER (PARTITION BY product_id ORDER BY month_index ROWS BETWEEN 6 PRECEDING AND 5 FOLLOWING) AS sum2
    FROM sales_with_index
),
with_ratio AS (
    SELECT product_id, mth, month_index, qty,
           qty * 24.0 / (sum1 + sum2) AS ratio
    FROM with_sums
    WHERE month_index BETWEEN 7 AND 30
),
ratios_2017 AS (
    SELECT product_id, mth, ratio
    FROM with_ratio
    WHERE strftime('%Y', mth) = '2017'
)
SELECT p.id, p.name
FROM products p
WHERE p.id IN (
    SELECT product_id
    FROM ratios_2017
    GROUP BY product_id
    HAVING COUNT(*) = 12 AND MIN(ratio) > 2
)
ORDER BY p.id;