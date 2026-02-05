WITH joined_data AS (
    SELECT 
        c."category_code",
        c."category_name",
        EXTRACT(YEAR FROM TRY_TO_TIMESTAMP(t."txn_date", 'YYYY-MM-DD HH24:MI:SS')) AS year_num,
        t."qty_sold(kg)" AS qty_sold_kg,
        t."unit_selling_px_rmb/kg" AS unit_selling_price,
        w."whsle_px_rmb-kg" AS wholesale_price,
        l."loss_rate_%" AS loss_rate_pct
    FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."VEG_TXN_DF" t
    INNER JOIN "BANK_SALES_TRADING"."BANK_SALES_TRADING"."VEG_WHSLE_DF" w 
        ON t."item_code" = w."item_code" 
        AND t."txn_date" = w."whsle_date"
    INNER JOIN "BANK_SALES_TRADING"."BANK_SALES_TRADING"."VEG_CAT" c 
        ON t."item_code" = c."item_code"
    INNER JOIN "BANK_SALES_TRADING"."BANK_SALES_TRADING"."VEG_LOSS_RATE_DF" l 
        ON t."item_code" = l."item_code"
    WHERE t."sale/return" = 'sale'
        AND year_num BETWEEN 2020 AND 2023
)
SELECT 
    "category_code",
    "category_name",
    year_num AS year,
    ROUND(AVG(wholesale_price), 2) AS average_wholesale_price,
    ROUND(MAX(wholesale_price), 2) AS maximum_wholesale_price,
    ROUND(MIN(wholesale_price), 2) AS minimum_wholesale_price,
    ROUND(MAX(wholesale_price) - MIN(wholesale_price), 2) AS wholesale_price_difference,
    ROUND(SUM(wholesale_price * qty_sold_kg / (1 - loss_rate_pct / 100)), 2) AS total_wholesale_price,
    ROUND(SUM(unit_selling_price * qty_sold_kg), 2) AS total_selling_price,
    ROUND(AVG(loss_rate_pct), 2) AS average_loss_rate,
    ROUND(SUM(wholesale_price * qty_sold_kg * (loss_rate_pct / 100) / (1 - loss_rate_pct / 100)), 2) AS total_loss,
    ROUND(SUM(unit_selling_price * qty_sold_kg) - SUM(wholesale_price * qty_sold_kg / (1 - loss_rate_pct / 100)), 2) AS profit
FROM joined_data
GROUP BY "category_code", "category_name", year_num
ORDER BY "category_code", year_num