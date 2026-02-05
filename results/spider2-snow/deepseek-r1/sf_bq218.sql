WITH revenue_2022 AS (
    SELECT "item_number", SUM("sale_dollars") as revenue_2022
    FROM "IOWA_LIQUOR_SALES"."IOWA_LIQUOR_SALES"."SALES"
    WHERE EXTRACT(YEAR FROM "date") = 2022
    GROUP BY "item_number"
),
revenue_2023 AS (
    SELECT "item_number", SUM("sale_dollars") as revenue_2023
    FROM "IOWA_LIQUOR_SALES"."IOWA_LIQUOR_SALES"."SALES"
    WHERE EXTRACT(YEAR FROM "date") = 2023
    GROUP BY "item_number"
),
item_growth AS (
    SELECT 
        r2022."item_number",
        r2022.revenue_2022,
        r2023.revenue_2023,
        ((r2023.revenue_2023 - r2022.revenue_2022) / r2022.revenue_2022) * 100 as growth_pct
    FROM revenue_2022 r2022
    INNER JOIN revenue_2023 r2023 ON r2022."item_number" = r2023."item_number"
    WHERE r2022.revenue_2022 > 0
),
distinct_items AS (
    SELECT DISTINCT "item_number", "item_description"
    FROM "IOWA_LIQUOR_SALES"."IOWA_LIQUOR_SALES"."SALES"
)
SELECT 
    di."item_description",
    ig.growth_pct
FROM item_growth ig
JOIN distinct_items di ON ig."item_number" = di."item_number"
ORDER BY ig.growth_pct DESC
LIMIT 5