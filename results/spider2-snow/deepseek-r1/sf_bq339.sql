WITH trips_2017 AS (
    SELECT 
        "duration_sec",
        TO_TIMESTAMP_NTZ("end_date" / 1000000) AS end_timestamp,
        "subscriber_type"
    FROM 
        "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_TRIPS"
    WHERE 
        "end_date" IS NOT NULL
),
monthly_totals AS (
    SELECT 
        EXTRACT(MONTH FROM end_timestamp) AS month_num,
        SUM(CASE WHEN "subscriber_type" = 'Customer' THEN "duration_sec" ELSE 0 END) / 60.0 AS customer_minutes,
        SUM(CASE WHEN "subscriber_type" = 'Subscriber' THEN "duration_sec" ELSE 0 END) / 60.0 AS subscriber_minutes
    FROM 
        trips_2017
    WHERE 
        EXTRACT(YEAR FROM end_timestamp) = 2017
    GROUP BY 
        month_num
),
differences AS (
    SELECT 
        month_num,
        ABS(customer_minutes - subscriber_minutes) / 1000 AS diff_thousands
    FROM 
        monthly_totals
)
SELECT 
    month_num
FROM 
    differences
ORDER BY 
    diff_thousands DESC
LIMIT 1