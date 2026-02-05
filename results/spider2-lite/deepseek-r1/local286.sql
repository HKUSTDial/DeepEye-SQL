WITH `seller_metrics` AS (
    SELECT 
        `seller_id`,
        COUNT(*) AS `total_quantity`,
        SUM(`price`) AS `total_sales`,
        AVG(`price`) AS `average_item_price`
    FROM `order_items`
    GROUP BY `seller_id`
    HAVING COUNT(*) > 100
),
`seller_packing` AS (
    SELECT 
        `oi`.`seller_id`,
        AVG((julianday(`o`.`order_delivered_carrier_date`) - julianday(`o`.`order_approved_at`)) * 24) AS `average_packing_hours`
    FROM `order_items` `oi`
    INNER JOIN `orders` `o` ON `oi`.`order_id` = `o`.`order_id`
    WHERE `o`.`order_approved_at` IS NOT NULL 
        AND `o`.`order_delivered_carrier_date` IS NOT NULL
    GROUP BY `oi`.`seller_id`
),
`seller_reviews` AS (
    SELECT 
        `oi`.`seller_id`,
        AVG(`orr`.`review_score`) AS `average_review_score`
    FROM `order_items` `oi`
    INNER JOIN `order_reviews` `orr` ON `oi`.`order_id` = `orr`.`order_id`
    GROUP BY `oi`.`seller_id`
),
`seller_category_quantity` AS (
    SELECT 
        `oi`.`seller_id`,
        `pct`.`product_category_name_english`,
        COUNT(*) AS `category_quantity`
    FROM `order_items` `oi`
    INNER JOIN `products` `p` ON `oi`.`product_id` = `p`.`product_id`
    INNER JOIN `product_category_name_translation` `pct` ON `p`.`product_category_name` = `pct`.`product_category_name`
    GROUP BY `oi`.`seller_id`, `pct`.`product_category_name_english`
),
`seller_top_category` AS (
    SELECT 
        `seller_id`,
        `product_category_name_english` AS `top_product_category_english`,
        ROW_NUMBER() OVER (PARTITION BY `seller_id` ORDER BY `category_quantity` DESC) AS `rn`
    FROM `seller_category_quantity`
)
SELECT 
    `s`.`seller_id`,
    `s`.`seller_city`,
    `s`.`seller_state`,
    `sm`.`total_sales`,
    `sm`.`average_item_price`,
    `sr`.`average_review_score`,
    `sp`.`average_packing_hours`,
    `stc`.`top_product_category_english`
FROM `sellers` `s`
INNER JOIN `seller_metrics` `sm` ON `s`.`seller_id` = `sm`.`seller_id`
LEFT JOIN `seller_packing` `sp` ON `s`.`seller_id` = `sp`.`seller_id`
LEFT JOIN `seller_reviews` `sr` ON `s`.`seller_id` = `sr`.`seller_id`
LEFT JOIN `seller_top_category` `stc` ON `s`.`seller_id` = `stc`.`seller_id` AND `stc`.`rn` = 1
ORDER BY `sm`.`total_sales` DESC