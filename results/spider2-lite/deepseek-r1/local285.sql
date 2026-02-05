WITH daily_txn AS (
    SELECT 
        t.`item_code`,
        c.`category_name`,
        l.`loss_rate_%`,
        strftime('%Y', t.`txn_date`) AS year,
        SUBSTR(t.`txn_date`, 1, 10) AS txn_date_only,
        SUM(t.`qty_sold(kg)`) AS total_qty_sold,
        SUM(t.`unit_selling_px_rmb/kg` * t.`qty_sold(kg)`) AS total_selling_amount
    FROM `veg_txn_df` t
    JOIN `veg_cat` c ON t.`item_code` = c.`item_code`
    JOIN `veg_loss_rate_df` l ON t.`item_code` = l.`item_code`
    WHERE t.`sale/return` = 'sale'
        AND strftime('%Y', t.`txn_date`) BETWEEN '2020' AND '2023'
    GROUP BY t.`item_code`, c.`category_name`, l.`loss_rate_%`, year, txn_date_only
),
whsle_daily AS (
    SELECT 
        `item_code`,
        SUBSTR(`whsle_date`, 1, 10) AS whsle_date_only,
        AVG(`whsle_px_rmb-kg`) AS avg_whsle_price,
        MAX(`whsle_px_rmb-kg`) AS max_whsle_price,
        MIN(`whsle_px_rmb-kg`) AS min_whsle_price
    FROM `veg_whsle_df`
    WHERE strftime('%Y', `whsle_date`) BETWEEN '2020' AND '2023'
    GROUP BY `item_code`, whsle_date_only
)
SELECT 
    d.`category_name`,
    d.`year`,
    ROUND(AVG(w.`avg_whsle_price`), 2) AS avg_wholesale_price,
    ROUND(MAX(w.`max_whsle_price`), 2) AS max_wholesale_price,
    ROUND(MIN(w.`min_whsle_price`), 2) AS min_wholesale_price,
    ROUND(MAX(w.`max_whsle_price`) - MIN(w.`min_whsle_price`), 2) AS wholesale_price_diff,
    ROUND(SUM(w.`avg_whsle_price` * d.`total_qty_sold`), 2) AS total_wholesale_price,
    ROUND(SUM(d.`total_selling_amount`), 2) AS total_selling_price,
    ROUND(AVG(d.`loss_rate_%`), 2) AS avg_loss_rate,
    ROUND(SUM((d.`loss_rate_%` / 100) * w.`avg_whsle_price` * d.`total_qty_sold`), 2) AS total_loss,
    ROUND(
        SUM(d.`total_selling_amount`) - 
        SUM(w.`avg_whsle_price` * d.`total_qty_sold`) - 
        SUM((d.`loss_rate_%` / 100) * w.`avg_whsle_price` * d.`total_qty_sold`)
    , 2) AS profit
FROM daily_txn d
JOIN whsle_daily w ON d.`item_code` = w.`item_code` AND d.`txn_date_only` = w.`whsle_date_only`
GROUP BY d.`category_name`, d.`year`
ORDER BY d.`category_name`, d.`year`