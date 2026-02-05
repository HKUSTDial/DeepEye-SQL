WITH collisions_2021 AS (
    SELECT pcf_violation_category, COUNT(*) AS count_2021
    FROM collisions
    WHERE substr(collision_date, 1, 4) = '2021' AND pcf_violation_category IS NOT NULL
    GROUP BY pcf_violation_category
),
total_2021 AS (
    SELECT COUNT(*) AS total_2021
    FROM collisions
    WHERE substr(collision_date, 1, 4) = '2021'
),
most_common_2021 AS (
    SELECT pcf_violation_category
    FROM collisions_2021
    ORDER BY count_2021 DESC
    LIMIT 1
),
collisions_2011 AS (
    SELECT pcf_violation_category, COUNT(*) AS count_2011
    FROM collisions
    WHERE substr(collision_date, 1, 4) = '2011' AND pcf_violation_category IS NOT NULL
    GROUP BY pcf_violation_category
),
total_2011 AS (
    SELECT COUNT(*) AS total_2011
    FROM collisions
    WHERE substr(collision_date, 1, 4) = '2011'
),
shares AS (
    SELECT 
        m.pcf_violation_category,
        COALESCE(c2021.count_2021, 0) AS count_2021,
        COALESCE(c2011.count_2011, 0) AS count_2011,
        t2021.total_2021,
        t2011.total_2011,
        (COALESCE(c2021.count_2021, 0) * 100.0 / t2021.total_2021) AS share_2021,
        (COALESCE(c2011.count_2011, 0) * 100.0 / t2011.total_2011) AS share_2011
    FROM most_common_2021 m
    LEFT JOIN collisions_2021 c2021 ON m.pcf_violation_category = c2021.pcf_violation_category
    LEFT JOIN collisions_2011 c2011 ON m.pcf_violation_category = c2011.pcf_violation_category
    CROSS JOIN total_2021 t2021
    CROSS JOIN total_2011 t2011
)
SELECT (share_2011 - share_2021) AS percentage_points_decrease FROM shares;