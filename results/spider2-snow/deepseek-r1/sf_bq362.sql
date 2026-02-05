WITH trips_2018 AS (
    SELECT 
        "company",
        TO_TIMESTAMP("trip_start_timestamp", 6) AS trip_start
    FROM "CHICAGO"."CHICAGO_TAXI_TRIPS"."TAXI_TRIPS"
    WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("trip_start_timestamp", 6)) = 2018
), monthly_counts AS (
    SELECT 
        "company",
        DATE_TRUNC('month', trip_start) AS month_start,
        COUNT(*) AS trip_count
    FROM trips_2018
    GROUP BY "company", DATE_TRUNC('month', trip_start)
), all_companies AS (
    SELECT DISTINCT "company" FROM monthly_counts
), all_months AS (
    SELECT DATEADD(month, ROW_NUMBER() OVER (ORDER BY 1) - 1, '2018-01-01') AS month_start
    FROM TABLE(GENERATOR(ROWCOUNT=>12))
), all_combinations AS (
    SELECT 
        ac."company",
        am.month_start
    FROM all_companies ac
    CROSS JOIN all_months am
), filled_counts AS (
    SELECT 
        ac."company",
        ac.month_start,
        COALESCE(mc.trip_count, 0) AS trip_count
    FROM all_combinations ac
    LEFT JOIN monthly_counts mc ON ac."company" = mc."company" AND ac.month_start = mc.month_start
), with_lead AS (
    SELECT 
        "company",
        month_start,
        trip_count,
        LEAD(trip_count) OVER (PARTITION BY "company" ORDER BY month_start) AS next_month_count
    FROM filled_counts
), increases AS (
    SELECT 
        "company",
        month_start,
        next_month_count - trip_count AS increase
    FROM with_lead
    WHERE next_month_count IS NOT NULL
), max_increases AS (
    SELECT 
        "company",
        MAX(increase) AS max_increase
    FROM increases
    WHERE increase > 0
    GROUP BY "company"
)
SELECT "company"
FROM max_increases
ORDER BY max_increase DESC
LIMIT 3