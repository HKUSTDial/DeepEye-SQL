WITH delivered_items AS (
    SELECT 
        oi.`seller_id`,
        oi.`order_id`,
        oi.`price`,
        oi.`freight_value`,
        o.`customer_id`
    FROM `olist_order_items` oi
    INNER JOIN `olist_orders` o ON oi.`order_id` = o.`order_id`
    WHERE o.`order_status` = 'delivered'
),
seller_customer_counts AS (
    SELECT 
        di.`seller_id`,
        COUNT(DISTINCT c.`customer_unique_id`) AS distinct_customers
    FROM delivered_items di
    INNER JOIN `olist_customers` c ON di.`customer_id` = c.`customer_id`
    GROUP BY di.`seller_id`
),
max_customers AS (
    SELECT MAX(distinct_customers) AS max_distinct_customers
    FROM seller_customer_counts
),
seller_profit AS (
    SELECT 
        `seller_id`,
        SUM(`price` - `freight_value`) AS total_profit
    FROM delivered_items
    GROUP BY `seller_id`
),
max_profit AS (
    SELECT MAX(total_profit) AS max_total_profit
    FROM seller_profit
),
seller_order_counts AS (
    SELECT 
        `seller_id`,
        COUNT(DISTINCT `order_id`) AS distinct_orders
    FROM delivered_items
    GROUP BY `seller_id`
),
max_orders AS (
    SELECT MAX(distinct_orders) AS max_distinct_orders
    FROM seller_order_counts
),
seller_five_star_counts AS (
    SELECT 
        di.`seller_id`,
        COUNT(CASE WHEN r.`review_score` = 5 THEN 1 END) AS five_star_count
    FROM delivered_items di
    LEFT JOIN `olist_order_reviews` r ON di.`order_id` = r.`order_id`
    GROUP BY di.`seller_id`
),
max_five_star AS (
    SELECT MAX(five_star_count) AS max_five_star_count
    FROM seller_five_star_counts
)
SELECT 
    'Most distinct customer unique IDs' AS achievement,
    scc.`seller_id`,
    scc.distinct_customers AS value
FROM seller_customer_counts scc
CROSS JOIN max_customers mc
WHERE scc.distinct_customers = mc.max_distinct_customers
UNION ALL
SELECT 
    'Highest profit' AS achievement,
    sp.`seller_id`,
    sp.total_profit AS value
FROM seller_profit sp
CROSS JOIN max_profit mp
WHERE sp.total_profit = mp.max_total_profit
UNION ALL
SELECT 
    'Most distinct orders' AS achievement,
    soc.`seller_id`,
    soc.distinct_orders AS value
FROM seller_order_counts soc
CROSS JOIN max_orders mo
WHERE soc.distinct_orders = mo.max_distinct_orders
UNION ALL
SELECT 
    'Most 5-star ratings' AS achievement,
    sfs.`seller_id`,
    sfs.five_star_count AS value
FROM seller_five_star_counts sfs
CROSS JOIN max_five_star mfs
WHERE sfs.five_star_count = mfs.max_five_star_count;