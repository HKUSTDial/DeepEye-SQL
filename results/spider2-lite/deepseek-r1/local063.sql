WITH us_sales AS (
    SELECT s.`cust_id`, s.`prod_id`, s.`time_id`, s.`amount_sold`,
           c.`cust_city_id`, c.`country_id`,
           t.`calendar_quarter_id`
    FROM `sales` s
    INNER JOIN `customers` c ON s.`cust_id` = c.`cust_id`
    INNER JOIN `countries` co ON c.`country_id` = co.`country_id`
    INNER JOIN `times` t ON s.`time_id` = t.`time_id`
    WHERE co.`country_name` = 'United States'
      AND s.`promo_id` = 999
      AND t.`calendar_quarter_id` IN (1772, 1776)
),
city_quarter_totals AS (
    SELECT `cust_city_id`, `calendar_quarter_id`,
           SUM(`amount_sold`) AS total_sales
    FROM us_sales
    GROUP BY `cust_city_id`, `calendar_quarter_id`
),
city_growth AS (
    SELECT `cust_city_id`,
           SUM(CASE WHEN `calendar_quarter_id` = 1772 THEN total_sales ELSE 0 END) AS sales_q4_2019,
           SUM(CASE WHEN `calendar_quarter_id` = 1776 THEN total_sales ELSE 0 END) AS sales_q4_2020
    FROM city_quarter_totals
    GROUP BY `cust_city_id`
    HAVING sales_q4_2019 > 0 
       AND sales_q4_2020 >= 1.2 * sales_q4_2019
),
filtered_sales AS (
    SELECT us.*
    FROM us_sales us
    INNER JOIN city_growth cg ON us.`cust_city_id` = cg.`cust_city_id`
),
product_totals AS (
    SELECT `prod_id`,
           SUM(`amount_sold`) AS total_sales
    FROM filtered_sales
    GROUP BY `prod_id`
),
product_ranks AS (
    SELECT `prod_id`, total_sales,
           ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS rn,
           COUNT(*) OVER () AS total_count
    FROM product_totals
),
top_products AS (
    SELECT `prod_id`
    FROM product_ranks
    WHERE rn <= CEIL(total_count * 0.2)
),
quarterly_product_sales AS (
    SELECT `prod_id`, `calendar_quarter_id`,
           SUM(`amount_sold`) AS product_amt
    FROM filtered_sales
    GROUP BY `prod_id`, `calendar_quarter_id`
),
quarterly_totals AS (
    SELECT `calendar_quarter_id`,
           SUM(`amount_sold`) AS quarter_total
    FROM filtered_sales
    GROUP BY `calendar_quarter_id`
),
product_quarter_shares AS (
    SELECT qps.`prod_id`,
           qps.`calendar_quarter_id`,
           qps.product_amt * 1.0 / qt.quarter_total AS share
    FROM quarterly_product_sales qps
    INNER JOIN quarterly_totals qt 
        ON qps.`calendar_quarter_id` = qt.`calendar_quarter_id`
),
product_share_pivot AS (
    SELECT `prod_id`,
           MAX(CASE WHEN `calendar_quarter_id` = 1772 THEN share ELSE NULL END) AS share_q4_2019,
           MAX(CASE WHEN `calendar_quarter_id` = 1776 THEN share ELSE NULL END) AS share_q4_2020
    FROM product_quarter_shares
    GROUP BY `prod_id`
    HAVING share_q4_2019 IS NOT NULL AND share_q4_2020 IS NOT NULL
),
share_changes AS (
    SELECT `prod_id`,
           share_q4_2020 - share_q4_2019 AS change_pp,
           ABS(share_q4_2020 - share_q4_2019) AS abs_change_pp
    FROM product_share_pivot
),
top_products_changes AS (
    SELECT sc.*
    FROM share_changes sc
    INNER JOIN top_products tp ON sc.`prod_id` = tp.`prod_id`
),
min_change_product AS (
    SELECT `prod_id`, change_pp, abs_change_pp
    FROM top_products_changes
    ORDER BY abs_change_pp ASC, `prod_id` ASC
    LIMIT 1
)
SELECT p.`prod_id`, p.`prod_name`
FROM min_change_product mcp
INNER JOIN `products` p ON mcp.`prod_id` = p.`prod_id`