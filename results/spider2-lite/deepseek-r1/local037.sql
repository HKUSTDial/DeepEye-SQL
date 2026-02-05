WITH order_categories AS (
    SELECT DISTINCT 
        `oi`.`order_id`, 
        `pt`.`product_category_name_english`
    FROM `olist_order_items` `oi`
    INNER JOIN `olist_products` `p` ON `oi`.`product_id` = `p`.`product_id`
    INNER JOIN `product_category_name_translation` `pt` ON `p`.`product_category_name` = `pt`.`product_category_name`
    WHERE `p`.`product_category_name` IS NOT NULL
),
payments_with_categories AS (
    SELECT 
        `oc`.`product_category_name_english`,
        `op`.`payment_type`
    FROM `order_categories` `oc`
    INNER JOIN `olist_order_payments` `op` ON `oc`.`order_id` = `op`.`order_id`
),
payment_counts AS (
    SELECT 
        `product_category_name_english`,
        `payment_type`,
        COUNT(*) AS `payment_count`
    FROM `payments_with_categories`
    GROUP BY `product_category_name_english`, `payment_type`
),
max_counts_per_category AS (
    SELECT 
        `product_category_name_english`,
        MAX(`payment_count`) AS `max_payment_count`
    FROM `payment_counts`
    GROUP BY `product_category_name_english`
)
SELECT 
    `product_category_name_english`,
    `max_payment_count`
FROM `max_counts_per_category`
ORDER BY `max_payment_count` DESC
LIMIT 3