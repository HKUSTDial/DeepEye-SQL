SELECT MAX("monthly_count") AS "highest_number_of_incidents"
FROM (
    SELECT COUNT(*) AS "monthly_count"
    FROM "CHICAGO"."CHICAGO_CRIME"."CRIME"
    WHERE "primary_type" = 'MOTOR VEHICLE THEFT' AND "year" = 2016
    GROUP BY MONTH(TO_TIMESTAMP_NTZ("date" / 1000000))
) monthly_counts