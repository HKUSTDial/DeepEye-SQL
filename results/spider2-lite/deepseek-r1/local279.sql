WITH RECURSIVE `products_with_min` AS (
    SELECT `product_id`, `qty_minimum`, `qty_purchase`
    FROM `product_minimums`
), `initial_inv` AS (
    SELECT 
        p.`product_id`,
        COALESCE(SUM(pur.`qty`), 0) - COALESCE(SUM(ms.`qty`), 0) AS `inv`
    FROM `products_with_min` p
    LEFT JOIN `purchases` pur ON p.`product_id` = pur.`product_id` AND pur.`purchased` <= '2018-12-31'
    LEFT JOIN `monthly_sales` ms ON p.`product_id` = ms.`product_id` AND ms.`mth` <= '2018-12-01'
    GROUP BY p.`product_id`
), `months` AS (
    SELECT '2019-01-01' AS `mth`
    UNION ALL
    SELECT date(`mth`, '+1 month')
    FROM `months`
    WHERE `mth` < '2019-12-01'
), `simulation` AS (
    SELECT 
        p.`product_id`,
        p.`qty_minimum`,
        p.`qty_purchase`,
        m.`mth`,
        i.`inv` AS `starting_inv`,
        COALESCE(s.`qty`, 0) AS `sales`,
        i.`inv` - COALESCE(s.`qty`, 0) AS `ending_before`,
        CASE WHEN i.`inv` - COALESCE(s.`qty`, 0) < p.`qty_minimum` THEN p.`qty_purchase` ELSE 0 END AS `restock_qty`,
        i.`inv` - COALESCE(s.`qty`, 0) + CASE WHEN i.`inv` - COALESCE(s.`qty`, 0) < p.`qty_minimum` THEN p.`qty_purchase` ELSE 0 END AS `ending_after`
    FROM `products_with_min` p
    CROSS JOIN (SELECT `mth` FROM `months` LIMIT 1) m
    LEFT JOIN `initial_inv` i ON p.`product_id` = i.`product_id`
    LEFT JOIN `monthly_sales` s ON p.`product_id` = s.`product_id` AND s.`mth` = m.`mth`
    UNION ALL
    SELECT 
        sim.`product_id`,
        sim.`qty_minimum`,
        sim.`qty_purchase`,
        next_m.`mth`,
        sim.`ending_after` AS `starting_inv`,
        COALESCE(s2.`qty`, 0) AS `sales`,
        sim.`ending_after` - COALESCE(s2.`qty`, 0) AS `ending_before`,
        CASE WHEN sim.`ending_after` - COALESCE(s2.`qty`, 0) < sim.`qty_minimum` THEN sim.`qty_purchase` ELSE 0 END AS `restock_qty`,
        sim.`ending_after` - COALESCE(s2.`qty`, 0) + CASE WHEN sim.`ending_after` - COALESCE(s2.`qty`, 0) < sim.`qty_minimum` THEN sim.`qty_purchase` ELSE 0 END AS `ending_after`
    FROM `simulation` sim
    JOIN `months` next_m ON next_m.`mth` = date(sim.`mth`, '+1 month')
    LEFT JOIN `monthly_sales` s2 ON sim.`product_id` = s2.`product_id` AND s2.`mth` = next_m.`mth`
    WHERE next_m.`mth` IS NOT NULL
), `abs_diffs` AS (
    SELECT 
        `product_id`,
        `mth`,
        ABS(`ending_before` - `qty_minimum`) AS `abs_diff`
    FROM `simulation`
), `ranked` AS (
    SELECT 
        `product_id`,
        `mth`,
        `abs_diff`,
        ROW_NUMBER() OVER (PARTITION BY `product_id` ORDER BY `abs_diff`, `mth`) AS `rn`
    FROM `abs_diffs`
)
SELECT `product_id`, `mth`, `abs_diff`
FROM `ranked`
WHERE `rn` = 1