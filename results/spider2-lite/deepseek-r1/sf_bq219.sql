WITH end_date_cte AS (
    SELECT DATEADD('DAY', -1, DATE_TRUNC('MONTH', CURRENT_DATE())) AS end_date
),
monthly_totals AS (
    SELECT 
        DATE_TRUNC('MONTH', "date") AS month,
        SUM("volume_sold_liters") AS total_volume
    FROM "IOWA_LIQUOR_SALES"."IOWA_LIQUOR_SALES"."SALES"
    WHERE "date" >= '2022-01-01' AND "date" <= (SELECT end_date FROM end_date_cte)
    GROUP BY DATE_TRUNC('MONTH', "date")
),
category_monthly AS (
    SELECT 
        DATE_TRUNC('MONTH', "date") AS month,
        "category_name",
        SUM("volume_sold_liters") AS cat_volume
    FROM "IOWA_LIQUOR_SALES"."IOWA_LIQUOR_SALES"."SALES"
    WHERE "date" >= '2022-01-01' AND "date" <= (SELECT end_date FROM end_date_cte)
    GROUP BY DATE_TRUNC('MONTH', "date"), "category_name"
),
monthly_pct AS (
    SELECT 
        cm.month,
        cm."category_name",
        cm.cat_volume,
        cm.cat_volume * 100.0 / mt.total_volume AS cat_pct
    FROM category_monthly cm
    JOIN monthly_totals mt ON cm.month = mt.month
),
eligible_categories AS (
    SELECT 
        "category_name",
        COUNT(*) AS months_count,
        AVG(cat_pct) AS avg_pct
    FROM monthly_pct
    GROUP BY "category_name"
    HAVING COUNT(*) >= 24 AND AVG(cat_pct) >= 1.0
),
category_pairs AS (
    SELECT 
        a."category_name" AS cat1,
        b."category_name" AS cat2,
        COUNT(*) AS n,
        SUM(a.cat_pct) AS sum_x,
        SUM(b.cat_pct) AS sum_y,
        SUM(a.cat_pct * b.cat_pct) AS sum_xy,
        SUM(a.cat_pct * a.cat_pct) AS sum_x2,
        SUM(b.cat_pct * b.cat_pct) AS sum_y2
    FROM monthly_pct a
    JOIN monthly_pct b ON a.month = b.month
    WHERE a."category_name" IN (SELECT "category_name" FROM eligible_categories)
      AND b."category_name" IN (SELECT "category_name" FROM eligible_categories)
      AND a."category_name" < b."category_name"
    GROUP BY a."category_name", b."category_name"
    HAVING COUNT(*) >= 24
),
correlations AS (
    SELECT 
        cat1,
        cat2,
        (n * sum_xy - sum_x * sum_y) / 
        (NULLIF(SQRT(n * sum_x2 - sum_x * sum_x), 0) * NULLIF(SQRT(n * sum_y2 - sum_y * sum_y), 0)) AS corr
    FROM category_pairs
)
SELECT cat1, cat2
FROM correlations
WHERE corr IS NOT NULL
ORDER BY corr ASC
LIMIT 1