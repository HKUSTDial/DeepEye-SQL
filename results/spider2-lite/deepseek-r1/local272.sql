WITH order_lines AS (
    SELECT 
        `id` AS orderline_id,
        `product_id`,
        `qty` AS demand,
        SUM(`qty`) OVER (PARTITION BY `product_id` ORDER BY `id`) AS cum_demand,
        COALESCE(SUM(`qty`) OVER (PARTITION BY `product_id` ORDER BY `id` ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS prev_cum_demand
    FROM `orderlines`
    WHERE `order_id` = 423
),
inventory_supply AS (
    SELECT 
        i.`product_id`,
        i.`location_id`,
        i.`qty` AS supply,
        p.`purchased`,
        l.`aisle`,
        l.`position`,
        SUM(i.`qty`) OVER (PARTITION BY i.`product_id` ORDER BY p.`purchased`, i.`qty`, i.`id`) AS cum_supply,
        COALESCE(SUM(i.`qty`) OVER (PARTITION BY i.`product_id` ORDER BY p.`purchased`, i.`qty`, i.`id` ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS prev_cum_supply
    FROM `inventory` i
    JOIN `purchases` p ON i.`purchase_id` = p.`id`
    JOIN `locations` l ON i.`location_id` = l.`id`
    WHERE l.`warehouse` = 1
),
allocation AS (
    SELECT 
        ol.orderline_id,
        ol.product_id,
        isup.aisle,
        isup.position,
        isup.purchased,
        isup.supply,
        MAX(0, MIN(ol.cum_demand, isup.cum_supply) - MAX(ol.prev_cum_demand, isup.prev_cum_supply)) AS qty_to_pick
    FROM order_lines ol
    JOIN inventory_supply isup ON ol.product_id = isup.product_id
    WHERE isup.prev_cum_supply < ol.cum_demand 
      AND ol.prev_cum_demand < isup.cum_supply
)
SELECT 
    orderline_id,
    product_id,
    aisle,
    position,
    qty_to_pick
FROM allocation
WHERE qty_to_pick > 0
ORDER BY orderline_id, purchased, supply