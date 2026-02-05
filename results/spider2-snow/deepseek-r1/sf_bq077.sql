WITH "years" AS (
    SELECT 2010 AS "year" UNION ALL
    SELECT 2011 UNION ALL
    SELECT 2012 UNION ALL
    SELECT 2013 UNION ALL
    SELECT 2014 UNION ALL
    SELECT 2015 UNION ALL
    SELECT 2016
),
"monthly_counts" AS (
    SELECT
        "year",
        EXTRACT(MONTH FROM TO_TIMESTAMP("date" / 1000000)) AS "month",
        COUNT(*) AS "theft_count"
    FROM "CHICAGO"."CHICAGO_CRIME"."CRIME"
    WHERE "primary_type" = 'MOTOR VEHICLE THEFT'
        AND "year" BETWEEN 2010 AND 2016
    GROUP BY "year", "month"
)
SELECT
    y."year",
    COALESCE(MAX(m."theft_count"), 0) AS "highest_monthly_thefts"
FROM "years" y
LEFT JOIN "monthly_counts" m ON y."year" = m."year"
GROUP BY y."year"
ORDER BY y."year";