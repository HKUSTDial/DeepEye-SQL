SELECT
    `country_code`,
    `country_name`,
    under25_pop / total_pop * 100 AS percentage
FROM (
    SELECT
        `country_code`,
        `country_name`,
        SUM(CASE WHEN `total_flag` = '*' THEN `midyear_population` ELSE 0 END) AS total_pop,
        SUM(CASE WHEN `total_flag` = 'A' AND `age_group_indicator` = '-' AND `starting_age` < 25 THEN `midyear_population` ELSE 0 END) AS under25_pop
    FROM `bigquery-public-data.census_bureau_international.midyear_population_5yr_age_sex`
    WHERE `year` = 2017
    GROUP BY `country_code`, `country_name`
)
WHERE total_pop > 0
ORDER BY percentage DESC
LIMIT 1