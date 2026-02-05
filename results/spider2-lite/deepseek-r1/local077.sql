WITH filtered_metrics AS (
    SELECT 
        interest_id,
        composition,
        index_value,
        _year,
        _month,
        month_year
    FROM interest_metrics
    WHERE month_year IS NOT NULL 
        AND ((_year = 2018 AND _month >= 9) OR (_year = 2019 AND _month <= 8))
),
avg_computation AS (
    SELECT 
        month_year,
        _year,
        _month,
        interest_id,
        composition / index_value AS avg_comp
    FROM filtered_metrics
    WHERE index_value != 0
),
ranked_interests AS (
    SELECT 
        month_year,
        _year,
        _month,
        interest_id,
        avg_comp,
        ROW_NUMBER() OVER (PARTITION BY month_year ORDER BY avg_comp DESC) AS rn
    FROM avg_computation
),
monthly_top AS (
    SELECT 
        month_year,
        _year,
        _month,
        interest_id,
        avg_comp AS max_index_composition
    FROM ranked_interests
    WHERE rn = 1
)
SELECT 
    mt.month_year AS `date`,
    im.interest_name,
    mt.max_index_composition,
    AVG(mt.max_index_composition) OVER (ORDER BY mt._year, mt._month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg,
    LAG(im.interest_name, 1) OVER (ORDER BY mt._year, mt._month) AS name_prev1,
    LAG(mt.max_index_composition, 1) OVER (ORDER BY mt._year, mt._month) AS max_comp_prev1,
    LAG(im.interest_name, 2) OVER (ORDER BY mt._year, mt._month) AS name_prev2,
    LAG(mt.max_index_composition, 2) OVER (ORDER BY mt._year, mt._month) AS max_comp_prev2
FROM monthly_top mt
JOIN interest_map im ON mt.interest_id = im.id
ORDER BY mt._year, mt._month;