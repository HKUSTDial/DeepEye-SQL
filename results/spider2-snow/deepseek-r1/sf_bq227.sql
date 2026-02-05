WITH "top_categories_2008" AS (
    SELECT "minor_category"
    FROM "LONDON"."LONDON_CRIME"."CRIME_BY_LSOA"
    WHERE "year" = 2008
    GROUP BY "minor_category"
    ORDER BY SUM("value") DESC
    LIMIT 5
),
"yearly_totals" AS (
    SELECT "year", SUM("value") AS "total_crimes"
    FROM "LONDON"."LONDON_CRIME"."CRIME_BY_LSOA"
    GROUP BY "year"
),
"yearly_category_values" AS (
    SELECT c."year", c."minor_category", SUM(c."value") AS "category_value"
    FROM "LONDON"."LONDON_CRIME"."CRIME_BY_LSOA" c
    INNER JOIN "top_categories_2008" t ON c."minor_category" = t."minor_category"
    GROUP BY c."year", c."minor_category"
),
"percentages" AS (
    SELECT yc."year", yc."minor_category", 
           ROUND(100.0 * yc."category_value" / yt."total_crimes", 2) AS "percentage_share"
    FROM "yearly_category_values" yc
    INNER JOIN "yearly_totals" yt ON yc."year" = yt."year"
)
SELECT "year",
       MAX(CASE WHEN "minor_category" = (SELECT "minor_category" FROM "top_categories_2008" LIMIT 1 OFFSET 0) THEN "percentage_share" ELSE 0.00 END) AS "category_1_share",
       MAX(CASE WHEN "minor_category" = (SELECT "minor_category" FROM "top_categories_2008" LIMIT 1 OFFSET 1) THEN "percentage_share" ELSE 0.00 END) AS "category_2_share",
       MAX(CASE WHEN "minor_category" = (SELECT "minor_category" FROM "top_categories_2008" LIMIT 1 OFFSET 2) THEN "percentage_share" ELSE 0.00 END) AS "category_3_share",
       MAX(CASE WHEN "minor_category" = (SELECT "minor_category" FROM "top_categories_2008" LIMIT 1 OFFSET 3) THEN "percentage_share" ELSE 0.00 END) AS "category_4_share",
       MAX(CASE WHEN "minor_category" = (SELECT "minor_category" FROM "top_categories_2008" LIMIT 1 OFFSET 4) THEN "percentage_share" ELSE 0.00 END) AS "category_5_share"
FROM "percentages"
GROUP BY "year"
ORDER BY "year"