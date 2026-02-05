WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('MONTH', "date") AS "month_start",
        "category_name",
        SUM("volume_sold_liters") AS "category_volume"
    FROM "IOWA_LIQUOR_SALES"."IOWA_LIQUOR_SALES"."SALES"
    WHERE "date" >= '2022-01-01'
        AND "date" < DATE_TRUNC('MONTH', CURRENT_DATE())
    GROUP BY 1, 2
),
monthly_totals AS (
    SELECT
        "month_start",
        SUM("category_volume") AS "total_volume"
    FROM monthly_sales
    GROUP BY 1
),
monthly_percentages AS (
    SELECT
        ms."month_start",
        ms."category_name",
        ms."category_volume",
        mt."total_volume",
        (ms."category_volume" / NULLIF(mt."total_volume", 0)) * 100 AS "percentage"
    FROM monthly_sales ms
    INNER JOIN monthly_totals mt ON ms."month_start" = mt."month_start"
),
category_stats AS (
    SELECT
        "category_name",
        AVG("percentage") AS "avg_percentage",
        COUNT(DISTINCT "month_start") AS "month_count"
    FROM monthly_percentages
    GROUP BY "category_name"
    HAVING "month_count" >= 24 AND "avg_percentage" >= 1
),
candidate_percentages AS (
    SELECT
        mp."month_start",
        mp."category_name",
        mp."percentage"
    FROM monthly_percentages mp
    INNER JOIN category_stats cs ON mp."category_name" = cs."category_name"
),
pairwise_correlations AS (
    SELECT
        cp1."category_name" AS "category1",
        cp2."category_name" AS "category2",
        CORR(cp1."percentage", cp2."percentage") AS "correlation"
    FROM candidate_percentages cp1
    INNER JOIN candidate_percentages cp2
        ON cp1."month_start" = cp2."month_start"
        AND cp1."category_name" < cp2."category_name"
    GROUP BY 1, 2
    HAVING "correlation" IS NOT NULL
)
SELECT
    "category1",
    "category2"
FROM pairwise_correlations
ORDER BY "correlation" ASC
LIMIT 1