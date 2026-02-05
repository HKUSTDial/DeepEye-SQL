WITH customer_profits AS (
  SELECT 
    cust."cust_id",
    SUM(s."amount_sold" - s."quantity_sold" * cs."unit_cost") AS profit
  FROM 
    "COMPLEX_ORACLE"."COMPLEX_ORACLE"."SALES" s
    INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COSTS" cs 
      ON s."prod_id" = cs."prod_id" 
      AND s."channel_id" = cs."channel_id" 
      AND s."promo_id" = cs."promo_id" 
      AND s."time_id" = cs."time_id"
    INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CUSTOMERS" cust 
      ON s."cust_id" = cust."cust_id"
    INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COUNTRIES" co 
      ON cust."country_id" = co."country_id"
    INNER JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."TIMES" t 
      ON s."time_id" = t."time_id"
  WHERE 
    co."country_name" = 'Italy'
    AND t."calendar_year" = 2021
    AND t."calendar_month_number" = 12
  GROUP BY 
    cust."cust_id"
),
tiered AS (
  SELECT 
    "cust_id",
    profit,
    NTILE(10) OVER (ORDER BY profit) AS tier
  FROM 
    customer_profits
)
SELECT 
  tier,
  MAX(profit) AS highest_profit,
  MIN(profit) AS lowest_profit
FROM 
  tiered
GROUP BY 
  tier
ORDER BY 
  tier