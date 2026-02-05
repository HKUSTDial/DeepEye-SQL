WITH 
count_1995 AS (
    SELECT UPPER(spc_latin) AS spc_upper, COUNT(*) AS cnt
    FROM `bigquery-public-data.new_york.tree_census_1995`
    WHERE status != 'Dead'
    GROUP BY UPPER(spc_latin)
),
count_2015 AS (
    SELECT UPPER(spc_latin) AS spc_upper, COUNT(*) AS cnt
    FROM `bigquery-public-data.new_york.tree_census_2015`
    WHERE status = 'Alive'
    GROUP BY UPPER(spc_latin)
)
SELECT 
    ts.fall_color,
    SUM(COALESCE(c1995.cnt, 0) - COALESCE(c2015.cnt, 0)) AS total_change
FROM `bigquery-public-data.new_york.tree_species` ts
LEFT JOIN count_1995 c1995 ON UPPER(ts.species_scientific_name) = c1995.spc_upper
LEFT JOIN count_2015 c2015 ON UPPER(ts.species_scientific_name) = c2015.spc_upper
GROUP BY ts.fall_color