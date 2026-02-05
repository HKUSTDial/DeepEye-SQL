WITH sales_2020 AS (
    SELECT 
        p.segment,
        COUNT(DISTINCT s.product_code) as product_count_2020
    FROM hardware_dim_product p
    JOIN hardware_fact_sales_monthly s ON p.product_code = s.product_code
    WHERE s.fiscal_year = 2020
    GROUP BY p.segment
),
sales_2021 AS (
    SELECT 
        p.segment,
        COUNT(DISTINCT s.product_code) as product_count_2021
    FROM hardware_dim_product p
    JOIN hardware_fact_sales_monthly s ON p.product_code = s.product_code
    WHERE s.fiscal_year = 2021
    GROUP BY p.segment
)
SELECT 
    s2020.segment,
    s2020.product_count_2020
FROM sales_2020 s2020
LEFT JOIN sales_2021 s2021 ON s2020.segment = s2021.segment
ORDER BY 
    CASE 
        WHEN s2020.product_count_2020 = 0 THEN 0
        ELSE ((COALESCE(s2021.product_count_2021, 0) - s2020.product_count_2020) * 100.0 / s2020.product_count_2020)
    END DESC