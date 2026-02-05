WITH us_customers AS (
    SELECT c.cust_id, c.cust_city
    FROM customers c
    JOIN countries co ON c.country_id = co.country_id
    WHERE co.country_name IN ('United States', 'USA', 'United States of America')
      AND c.cust_valid = 'A'
),
sales_no_promo AS (
    SELECT s.cust_id, s.time_id, s.amount_sold, s.prod_id, t.calendar_year, t.calendar_quarter_number
    FROM sales s
    JOIN times t ON s.time_id = t.time_id
    WHERE s.promo_id = 999
      AND ((t.calendar_year = 2019 AND t.calendar_quarter_number = 4)
        OR (t.calendar_year = 2020 AND t.calendar_quarter_number = 4))
),
us_sales AS (
    SELECT snp.cust_id, snp.amount_sold, snp.prod_id, snp.calendar_year, uc.cust_city
    FROM sales_no_promo snp
    JOIN us_customers uc ON snp.cust_id = uc.cust_id
),
city_quarter_totals AS (
    SELECT cust_city, calendar_year, SUM(amount_sold) AS total_sales
    FROM us_sales
    GROUP BY cust_city, calendar_year
),
city_growth AS (
    SELECT cust_city,
        MAX(CASE WHEN calendar_year = 2019 THEN total_sales END) AS sales_2019,
        MAX(CASE WHEN calendar_year = 2020 THEN total_sales END) AS sales_2020,
        (COALESCE(MAX(CASE WHEN calendar_year = 2020 THEN total_sales END), 0) - COALESCE(MAX(CASE WHEN calendar_year = 2019 THEN total_sales END), 0)) * 1.0 / COALESCE(MAX(CASE WHEN calendar_year = 2019 THEN total_sales END), 1) AS growth_rate
    FROM city_quarter_totals
    GROUP BY cust_city
    HAVING sales_2019 > 0 AND sales_2020 > 0 AND growth_rate >= 0.20
),
sales_qualifying_cities AS (
    SELECT us.cust_city, us.calendar_year, us.amount_sold, us.prod_id
    FROM us_sales us
    WHERE us.cust_city IN (SELECT cust_city FROM city_growth)
),
product_overall AS (
    SELECT prod_id, SUM(amount_sold) AS overall_sales
    FROM sales_qualifying_cities
    GROUP BY prod_id
),
product_rank AS (
    SELECT prod_id, overall_sales,
        ROW_NUMBER() OVER (ORDER BY overall_sales DESC) AS rn,
        COUNT(*) OVER () AS total_count
    FROM product_overall
),
top_products AS (
    SELECT prod_id, overall_sales
    FROM product_rank
    WHERE rn <= (total_count + 4) / 5
),
quarter_totals AS (
    SELECT calendar_year, SUM(amount_sold) AS quarter_total
    FROM sales_qualifying_cities
    GROUP BY calendar_year
),
product_quarter_sales AS (
    SELECT tp.prod_id, sqc.calendar_year, SUM(sqc.amount_sold) AS product_sales
    FROM sales_qualifying_cities sqc
    JOIN top_products tp ON sqc.prod_id = tp.prod_id
    GROUP BY tp.prod_id, sqc.calendar_year
),
product_shares AS (
    SELECT pqs.prod_id, pqs.calendar_year, pqs.product_sales, qt.quarter_total,
        1.0 * pqs.product_sales / qt.quarter_total AS share
    FROM product_quarter_sales pqs
    JOIN quarter_totals qt ON pqs.calendar_year = qt.calendar_year
),
share_pivot AS (
    SELECT prod_id,
        MAX(CASE WHEN calendar_year = 2019 THEN share END) AS share_2019,
        MAX(CASE WHEN calendar_year = 2020 THEN share END) AS share_2020
    FROM product_shares
    GROUP BY prod_id
)
SELECT sp.prod_id, p.prod_name, sp.share_2019, sp.share_2020, (sp.share_2020 - sp.share_2019) AS share_change
FROM share_pivot sp
JOIN products p ON sp.prod_id = p.prod_id
ORDER BY share_change DESC;