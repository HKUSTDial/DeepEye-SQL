WITH region_aggregates AS (
    SELECT 
        r.id AS region_id,
        r.name AS region_name,
        COUNT(o.id) AS num_orders,
        COALESCE(SUM(o.total_amt_usd), 0) AS total_sales_region
    FROM web_region r
    LEFT JOIN web_sales_reps sr ON r.id = sr.region_id
    LEFT JOIN web_accounts a ON sr.id = a.sales_rep_id
    LEFT JOIN web_orders o ON a.id = o.account_id
    GROUP BY r.id, r.name
),
rep_sales AS (
    SELECT 
        sr.region_id,
        sr.id AS rep_id,
        sr.name AS rep_name,
        SUM(o.total_amt_usd) AS rep_total_sales
    FROM web_sales_reps sr
    JOIN web_accounts a ON sr.id = a.sales_rep_id
    JOIN web_orders o ON a.id = o.account_id
    GROUP BY sr.region_id, sr.id, sr.name
),
max_rep_sales_per_region AS (
    SELECT 
        region_id,
        MAX(rep_total_sales) AS max_rep_sales
    FROM rep_sales
    GROUP BY region_id
),
top_reps AS (
    SELECT 
        rs.region_id,
        rs.rep_name,
        rs.rep_total_sales
    FROM rep_sales rs
    JOIN max_rep_sales_per_region mr ON rs.region_id = mr.region_id AND rs.rep_total_sales = mr.max_rep_sales
)
SELECT 
    ra.region_name,
    ra.num_orders,
    ra.total_sales_region,
    tr.rep_name AS top_rep_name,
    tr.rep_total_sales AS top_rep_sales_amount
FROM region_aggregates ra
LEFT JOIN top_reps tr ON ra.region_id = tr.region_id
ORDER BY ra.region_name, tr.rep_name;