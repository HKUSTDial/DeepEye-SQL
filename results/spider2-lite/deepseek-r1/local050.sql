WITH historical_sales AS (
    SELECT 
        s.prod_id,
        CAST(strftime('%m', s.time_id) AS INTEGER) as month_num,
        CAST(strftime('%Y', s.time_id) AS INTEGER) as year,
        SUM(s.amount_sold) as monthly_sales
    FROM sales s
    JOIN promotions p ON s.promo_id = p.promo_id
    JOIN channels c ON s.channel_id = c.channel_id
    JOIN customers cust ON s.cust_id = cust.cust_id
    JOIN countries cntry ON cust.country_id = cntry.country_id
    WHERE cntry.country_name = 'France'
        AND p.promo_total_id = 1
        AND c.channel_total_id = 1
        AND CAST(strftime('%Y', s.time_id) AS INTEGER) IN (2019, 2020)
    GROUP BY s.prod_id, month_num, year
),
projected_sales AS (
    SELECT 
        prod_id,
        month_num,
        CASE 
            WHEN sales_2019 > 0 THEN 
                ((sales_2020 - sales_2019) / sales_2019 * sales_2020) + sales_2020
            ELSE sales_2020 
        END as projected_sales_local
    FROM (
        SELECT 
            prod_id,
            month_num,
            MAX(CASE WHEN year = 2019 THEN monthly_sales END) as sales_2019,
            MAX(CASE WHEN year = 2020 THEN monthly_sales END) as sales_2020
        FROM historical_sales
        GROUP BY prod_id, month_num
    ) 
    WHERE sales_2019 IS NOT NULL AND sales_2020 IS NOT NULL
),
projected_usd_sales AS (
    SELECT 
        p.prod_id,
        p.month_num,
        p.projected_sales_local * COALESCE(curr.to_us, 1) as projected_sales_usd
    FROM projected_sales p
    LEFT JOIN currency curr ON curr.country = 'France' 
        AND curr.year = 2021 
        AND curr.month = p.month_num
),
monthly_averages AS (
    SELECT 
        month_num,
        AVG(projected_sales_usd) as avg_monthly_projected_usd
    FROM projected_usd_sales
    GROUP BY month_num
)
SELECT 
    AVG(avg_monthly_projected_usd) as median_value
FROM (
    SELECT 
        avg_monthly_projected_usd,
        ROW_NUMBER() OVER (ORDER BY avg_monthly_projected_usd) as row_num,
        COUNT(*) OVER () as total_count
    FROM monthly_averages
) WHERE row_num IN ((total_count + 1) / 2, (total_count + 2) / 2)