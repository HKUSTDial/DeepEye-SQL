WITH category_year_avg AS (
    SELECT 
        "category_name",
        EXTRACT(YEAR FROM "date") AS "year",
        AVG("sale_dollars" / "volume_sold_liters") AS "avg_price"
    FROM "IOWA_LIQUOR_SALES"."IOWA_LIQUOR_SALES"."SALES"
    WHERE "date" >= '2019-01-01' AND "date" < '2022-01-01'
    GROUP BY "category_name", "year"
),
top_categories_2021 AS (
    SELECT "category_name"
    FROM category_year_avg
    WHERE "year" = 2021
    ORDER BY "avg_price" DESC
    LIMIT 10
)
SELECT 
    t."category_name",
    MAX(CASE WHEN c."year" = 2019 THEN c."avg_price" END) AS "avg_price_2019",
    MAX(CASE WHEN c."year" = 2020 THEN c."avg_price" END) AS "avg_price_2020",
    MAX(CASE WHEN c."year" = 2021 THEN c."avg_price" END) AS "avg_price_2021"
FROM top_categories_2021 t
JOIN category_year_avg c ON t."category_name" = c."category_name"
GROUP BY t."category_name"
ORDER BY "avg_price_2021" DESC