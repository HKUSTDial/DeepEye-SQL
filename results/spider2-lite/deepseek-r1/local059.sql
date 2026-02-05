SELECT AVG(total_qty) AS overall_avg_qty
FROM (
    SELECT division, product_code, total_qty,
           ROW_NUMBER() OVER (PARTITION BY division ORDER BY total_qty DESC, product_code) AS rank
    FROM (
        SELECT p.division, s.product_code, SUM(s.sold_quantity) AS total_qty
        FROM hardware_fact_sales_monthly s
        INNER JOIN hardware_dim_product p ON s.product_code = p.product_code
        WHERE strftime('%Y', s.date) = '2021'
        GROUP BY p.division, s.product_code
    ) product_totals
) ranked
WHERE rank <= 3