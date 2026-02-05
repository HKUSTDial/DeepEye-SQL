WITH italian_customers AS (
    SELECT c."cust_id"
    FROM "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CUSTOMERS" c
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COUNTRIES" co ON c."country_id" = co."country_id"
    WHERE co."country_name" = 'Italy'
),
dec_2021_sales AS (
    SELECT s."cust_id", s."quantity_sold", s."prod_id", s."channel_id", s."promo_id", s."time_id"
    FROM "COMPLEX_ORACLE"."COMPLEX_ORACLE"."SALES" s
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."TIMES" t ON s."time_id" = t."time_id"
    WHERE t."calendar_year" = 2021 AND t."calendar_month_number" = 12
),
sales_with_costs AS (
    SELECT d."cust_id", 
           d."quantity_sold" * (c."unit_price" - c."unit_cost") AS profit
    FROM dec_2021_sales d
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COSTS" c 
        ON d."prod_id" = c."prod_id"
        AND d."channel_id" = c."channel_id"
        AND d."promo_id" = c."promo_id"
        AND d."time_id" = c."time_id"
),
customer_profits AS (
    SELECT swc."cust_id", SUM(swc.profit) AS total_profit
    FROM sales_with_costs swc
    WHERE swc."cust_id" IN (SELECT "cust_id" FROM italian_customers)
    GROUP BY swc."cust_id"
),
profit_range AS (
    SELECT MIN(total_profit) AS min_profit, MAX(total_profit) AS max_profit
    FROM customer_profits
),
buckets AS (
    SELECT cp."cust_id", cp.total_profit,
           WIDTH_BUCKET(cp.total_profit, pr.min_profit, pr.max_profit, 10) AS bucket_num
    FROM customer_profits cp
    CROSS JOIN profit_range pr
)
SELECT bucket_num AS bucket,
       COUNT(DISTINCT "cust_id") AS number_of_customers,
       MIN(total_profit) AS minimum_total_profit,
       MAX(total_profit) AS maximum_total_profit
FROM buckets
GROUP BY bucket_num
ORDER BY bucket_num