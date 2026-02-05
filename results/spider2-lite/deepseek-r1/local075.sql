SELECT 
    product_id,
    page_name,
    SUM(views) AS total_views,
    SUM(adds) AS total_adds,
    SUM(purchases) AS total_purchases,
    SUM(CASE WHEN adds > purchases THEN adds - purchases ELSE 0 END) AS abandoned_adds
FROM (
    SELECT 
        h.product_id,
        h.page_name,
        e.visit_id,
        SUM(CASE WHEN i.event_name = 'Page View' THEN 1 ELSE 0 END) AS views,
        SUM(CASE WHEN i.event_name = 'Add to Cart' THEN 1 ELSE 0 END) AS adds,
        SUM(CASE WHEN i.event_name = 'Purchase' THEN 1 ELSE 0 END) AS purchases
    FROM shopping_cart_events e
    JOIN shopping_cart_page_hierarchy h ON e.page_id = h.page_id
    JOIN shopping_cart_event_identifier i ON e.event_type = i.event_type
    WHERE e.page_id NOT IN (1,2,12,13)
    GROUP BY h.product_id, h.page_name, e.visit_id
) per_product_visit
GROUP BY product_id, page_name
ORDER BY product_id