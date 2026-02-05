WITH daily_counts AS (
    SELECT "year", "month", "day", COUNT(*) AS sighting_count
    FROM "GBIF"."GBIF"."OCCURRENCES"
    WHERE "species" = 'Sterna paradisaea'
        AND "decimallatitude" > 40
        AND "month" >= 2
    GROUP BY "year", "month", "day"
    HAVING sighting_count > 10
),
year_earliest AS (
    SELECT "year", MIN(DATE_FROM_PARTS("year", "month", "day")) AS earliest_date
    FROM daily_counts
    GROUP BY "year"
)
SELECT "year"
FROM year_earliest
ORDER BY earliest_date
LIMIT 1