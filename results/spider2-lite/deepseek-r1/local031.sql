WITH annual_volumes AS (
    SELECT 
        strftime('%Y', `order_delivered_customer_date`) AS `year`,
        COUNT(*) AS `annual_count`
    FROM `olist_orders`
    WHERE `order_status` = 'delivered'
        AND `order_delivered_customer_date` IS NOT NULL
        AND strftime('%Y', `order_delivered_customer_date`) IN ('2016', '2017', '2018')
    GROUP BY `year`
),
min_year AS (
    SELECT `year`
    FROM annual_volumes
    ORDER BY `annual_count` ASC
    LIMIT 1
),
monthly_volumes AS (
    SELECT 
        strftime('%m', `order_delivered_customer_date`) AS `month`,
        COUNT(*) AS `monthly_count`
    FROM `olist_orders`
    WHERE `order_status` = 'delivered'
        AND `order_delivered_customer_date` IS NOT NULL
        AND strftime('%Y', `order_delivered_customer_date`) = (SELECT `year` FROM min_year)
    GROUP BY `month`
)
SELECT MAX(`monthly_count`) AS `highest_monthly_volume`
FROM monthly_volumes;