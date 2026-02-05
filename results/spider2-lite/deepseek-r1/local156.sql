WITH btc_buys AS (
    SELECT 
        m.`region`,
        SUBSTR(t.`txn_date`, 7, 4) AS `year`,
        t.`quantity`,
        p.`price`
    FROM `bitcoin_transactions` t
    JOIN `bitcoin_members` m ON t.`member_id` = m.`member_id`
    JOIN `bitcoin_prices` p ON t.`ticker` = p.`ticker` AND t.`txn_date` = p.`market_date`
    WHERE t.`ticker` = 'BTC' AND t.`txn_type` = 'BUY'
),
region_year_totals AS (
    SELECT 
        `region`,
        `year`,
        SUM(`quantity` * `price`) AS `total_dollar_amount`,
        SUM(`quantity`) AS `total_quantity`
    FROM btc_buys
    GROUP BY `region`, `year`
    HAVING `total_quantity` > 0
),
avg_prices AS (
    SELECT 
        `region`,
        `year`,
        `total_dollar_amount` / `total_quantity` AS `avg_price`
    FROM region_year_totals
),
with_first_year AS (
    SELECT 
        *,
        MIN(`year`) OVER (PARTITION BY `region`) AS `first_year`
    FROM avg_prices
),
filtered_avg_prices AS (
    SELECT 
        `region`,
        `year`,
        `avg_price`
    FROM with_first_year
    WHERE `year` != `first_year`
)
SELECT 
    `year`,
    `region`,
    `avg_price`,
    RANK() OVER (PARTITION BY `year` ORDER BY `avg_price` DESC) AS `rank`,
    ((`avg_price` - LAG(`avg_price`) OVER (PARTITION BY `region` ORDER BY `year`)) / LAG(`avg_price`) OVER (PARTITION BY `region` ORDER BY `year`)) * 100 AS `pct_change`
FROM filtered_avg_prices
ORDER BY `year`, `rank`;