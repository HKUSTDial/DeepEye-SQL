WITH customer_profit AS (
    SELECT c.`cust_id`,
           SUM(s.`amount_sold` - s.`quantity_sold` * co.`unit_cost`) AS total_profit
    FROM `customers` c
    JOIN `countries` cnt ON c.`country_id` = cnt.`country_id`
    JOIN `sales` s ON c.`cust_id` = s.`cust_id`
    JOIN `times` t ON s.`time_id` = t.`time_id`
    JOIN `costs` co ON s.`channel_id` = co.`channel_id`
                    AND s.`time_id` = co.`time_id`
                    AND s.`promo_id` = co.`promo_id`
                    AND s.`prod_id` = co.`prod_id`
    WHERE cnt.`country_name` = 'Italy'
      AND t.`calendar_year` = 2021
      AND t.`calendar_month_number` = 12
    GROUP BY c.`cust_id`
),
tiered AS (
    SELECT `cust_id`,
           `total_profit`,
           NTILE(10) OVER (ORDER BY `total_profit`) AS `tier`
    FROM customer_profit
)
SELECT `tier`,
       MIN(`total_profit`) AS `lowest_profit`,
       MAX(`total_profit`) AS `highest_profit`
FROM tiered
GROUP BY `tier`
ORDER BY `tier`