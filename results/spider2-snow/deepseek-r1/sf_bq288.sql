WITH filtered_institutions AS (
    SELECT "state", SUM("total_assets") AS total_assets_sum
    FROM "FDA"."FDIC_BANKS"."INSTITUTIONS"
    WHERE "established_date" BETWEEN '1900-01-01' AND '2000-12-31'
        AND "institution_name" LIKE 'Bank%'
    GROUP BY "state"
),
max_state AS (
    SELECT "state"
    FROM filtered_institutions
    ORDER BY total_assets_sum DESC
    LIMIT 1
)
SELECT COUNT(*) AS total_number_of_institutions
FROM "FDA"."FDIC_BANKS"."INSTITUTIONS"
WHERE "state" = (SELECT "state" FROM max_state)