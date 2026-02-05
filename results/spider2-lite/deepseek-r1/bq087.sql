SELECT 
    (avg_2020 - avg_2019) / avg_2019 * 100 AS percentage_change
FROM (
    SELECT
        AVG(CASE WHEN year = 2019 THEN freq END) AS avg_2019,
        AVG(CASE WHEN year = 2020 THEN freq END) AS avg_2020
    FROM (
        SELECT 
            EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS year,
            SAFE_CAST(symptom_anosmia AS FLOAT64) AS freq
        FROM `bigquery-public-data.covid19_symptom_search.symptom_search_sub_region_2_weekly`
        WHERE sub_region_1 = 'New York'
            AND sub_region_2 IN ('Bronx County', 'Queens County', 'Kings County', 'New York County', 'Richmond County')
            AND ((date >= '2019-01-01' AND date <= '2019-12-31') OR (date >= '2020-01-01' AND date <= '2020-12-31'))
            AND SAFE_CAST(symptom_anosmia AS FLOAT64) IS NOT NULL
    )
)