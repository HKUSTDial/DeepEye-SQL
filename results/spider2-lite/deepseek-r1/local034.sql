WITH distinct_payments_per_category AS (
    SELECT DISTINCT
        `p`.`product_category_name`,
        `pay`.`payment_type`,
        `pay`.`order_id`,
        `pay`.`payment_sequential`
    FROM `olist_order_payments` `pay`
    JOIN `olist_order_items` `items` ON `pay`.`order_id` = `items`.`order_id`
    JOIN `olist_products` `p` ON `items`.`product_id` = `p`.`product_id`
    WHERE `p`.`product_category_name` IS NOT NULL
),
category_payment_counts AS (
    SELECT
        `product_category_name`,
        `payment_type`,
        COUNT(*) AS `payment_count`
    FROM distinct_payments_per_category
    GROUP BY `product_category_name`, `payment_type`
),
category_max AS (
    SELECT
        `product_category_name`,
        MAX(`payment_count`) AS `max_payment_count`
    FROM category_payment_counts
    GROUP BY `product_category_name`
)
SELECT AVG(`max_payment_count`) AS `average_max_payments`
FROM category_max