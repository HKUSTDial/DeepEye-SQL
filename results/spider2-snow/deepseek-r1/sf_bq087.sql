WITH filtered_data AS (
    SELECT 
        TO_DATE("date", 'YYYY-MM-DD') AS week_date,
        EXTRACT(YEAR FROM TO_DATE("date", 'YYYY-MM-DD')) AS year,
        COALESCE(TRY_TO_DOUBLE("symptom_anosmia"), 0) AS symptom_value
    FROM "COVID19_SYMPTOM_SEARCH"."COVID19_SYMPTOM_SEARCH"."SYMPTOM_SEARCH_SUB_REGION_2_WEEKLY"
    WHERE "sub_region_1" = 'New York'
        AND "sub_region_2" IN ('Bronx County', 'Queens County', 'Kings County', 'New York County', 'Richmond County')
        AND EXTRACT(YEAR FROM TO_DATE("date", 'YYYY-MM-DD')) IN (2019, 2020)
),
weekly_sums AS (
    SELECT 
        year,
        week_date,
        SUM(symptom_value) AS weekly_sum
    FROM filtered_data
    GROUP BY year, week_date
)
SELECT 
    (AVG(CASE WHEN year = 2020 THEN weekly_sum END) - AVG(CASE WHEN year = 2019 THEN weekly_sum END)) / NULLIF(AVG(CASE WHEN year = 2019 THEN weekly_sum END), 0) * 100 AS percentage_change
FROM weekly_sums