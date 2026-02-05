WITH historical_sales AS (
    SELECT 
        s.prod_id,
        t.calendar_month_number as month,
        t.calendar_year as year,
        SUM(s.amount_sold) as total_sales
    FROM sales s
    JOIN customers c ON s.cust_id = c.cust_id
    JOIN countries co ON c.country_id = co.country_id AND co.country_name = 'France'
    JOIN promotions p ON s.promo_id = p.promo_id AND p.promo_total_id = 1
    JOIN channels ch ON s.channel_id = ch.channel_id AND ch.channel_total_id = 1
    JOIN times t ON s.time_id = t.time_id
    WHERE t.calendar_year IN (2019, 2020)
    GROUP BY s.prod_id, t.calendar_month_number, t.calendar_year
),
sales_pivot AS (
    SELECT 
        prod_id,
        month,
        MAX(CASE WHEN year = 2019 THEN total_sales END) as sales_2019,
        MAX(CASE WHEN year = 2020 THEN total_sales END) as sales_2020
    FROM historical_sales
    GROUP BY prod_id, month
    HAVING sales_2019 IS NOT NULL AND sales_2020 IS NOT NULL AND sales_2019 > 0
),
projected_local AS (
    SELECT 
        prod_id,
        month,
        sales_2020 * sales_2020 / sales_2019 as projected_sales_local
    FROM sales_pivot
),
projected_usd AS (
    SELECT 
        p.prod_id,
        p.month,
        p.projected_sales_local * c.to_us as projected_sales_usd
    FROM projected_local p
    JOIN currency c ON c.year = 2021 AND c.month = p.month AND c.country = 'France'
)
SELECT 
    month,
    ROUND(AVG(projected_sales_usd), 2) as avg_projected_monthly_sales_usd
FROM projected_usd
GROUP BY month
ORDER BY month