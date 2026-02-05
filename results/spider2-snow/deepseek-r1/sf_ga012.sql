WITH purchase_items AS (
    SELECT 
        e."ECOMMERCE":"transaction_id"::STRING AS transaction_id,
        f.value:"item_category"::STRING AS item_category,
        f.value:"quantity"::INT AS quantity,
        f.value:"item_revenue_in_usd"::FLOAT AS item_revenue_in_usd,
        f.value:"tax_in_usd"::FLOAT AS tax_in_usd,
        f.value:"item_revenue"::FLOAT AS item_revenue
    FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201130" e,
    LATERAL FLATTEN(INPUT => e."ITEMS") f
    WHERE e."EVENT_NAME" = 'purchase'
),
category_stats AS (
    SELECT 
        item_category,
        SUM(tax_in_usd) AS total_tax_usd,
        SUM(item_revenue_in_usd) AS total_revenue_usd,
        total_tax_usd / NULLIF(total_revenue_usd, 0) AS tax_rate
    FROM purchase_items
    WHERE item_category IS NOT NULL
    GROUP BY item_category
    HAVING total_revenue_usd > 0
),
top_category AS (
    SELECT item_category
    FROM category_stats
    ORDER BY tax_rate DESC
    LIMIT 1
)
SELECT 
    p.transaction_id,
    SUM(p.quantity) AS total_item_quantities,
    SUM(p.item_revenue_in_usd) AS purchase_revenue_in_usd,
    SUM(p.item_revenue) AS purchase_revenue
FROM purchase_items p
INNER JOIN top_category t ON p.item_category = t.item_category
GROUP BY p.transaction_id
ORDER BY p.transaction_id