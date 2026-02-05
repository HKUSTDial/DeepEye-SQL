WITH filtered AS (
    SELECT 
        t.`boroname`,
        t.`tree_id`,
        i.`Estimate_Mean_income`
    FROM `trees` t
    INNER JOIN `income_trees` i ON t.`zipcode` = i.`zipcode`
    WHERE i.`Estimate_Median_income` > 0 
      AND i.`Estimate_Mean_income` > 0
      AND t.`boroname` IS NOT NULL
      AND t.`boroname` != ''
),
borough_stats AS (
    SELECT 
        `boroname`,
        COUNT(`tree_id`) AS `tree_count`,
        AVG(`Estimate_Mean_income`) AS `avg_mean_income`
    FROM filtered
    GROUP BY `boroname`
),
ranked AS (
    SELECT 
        `boroname`,
        `tree_count`,
        `avg_mean_income`,
        ROW_NUMBER() OVER (ORDER BY `tree_count` DESC, `boroname`) AS `row_num`
    FROM borough_stats
)
SELECT 
    `boroname`,
    `avg_mean_income`
FROM ranked
WHERE `row_num` <= 3
ORDER BY `tree_count` DESC