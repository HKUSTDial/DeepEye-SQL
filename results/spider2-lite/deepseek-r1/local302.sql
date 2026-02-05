WITH filtered_sales AS (
    SELECT 
        `week_date`,
        `region`,
        `platform`,
        `age_band`,
        `demographic`,
        `customer_type`,
        `sales`
    FROM `cleaned_weekly_sales`
    WHERE (`week_date` BETWEEN '2020-03-23' AND '2020-06-08')
       OR (`week_date` BETWEEN '2020-06-15' AND '2020-08-31')
),
region_data AS (
    SELECT 
        'region' AS attribute_type,
        `region` AS attribute_value,
        SUM(CASE WHEN `week_date` <= '2020-06-08' THEN `sales` ELSE 0 END) AS before_sales,
        SUM(CASE WHEN `week_date` >= '2020-06-15' THEN `sales` ELSE 0 END) AS after_sales
    FROM filtered_sales
    GROUP BY `region`
),
platform_data AS (
    SELECT 
        'platform' AS attribute_type,
        `platform` AS attribute_value,
        SUM(CASE WHEN `week_date` <= '2020-06-08' THEN `sales` ELSE 0 END) AS before_sales,
        SUM(CASE WHEN `week_date` >= '2020-06-15' THEN `sales` ELSE 0 END) AS after_sales
    FROM filtered_sales
    GROUP BY `platform`
),
age_band_data AS (
    SELECT 
        'age_band' AS attribute_type,
        `age_band` AS attribute_value,
        SUM(CASE WHEN `week_date` <= '2020-06-08' THEN `sales` ELSE 0 END) AS before_sales,
        SUM(CASE WHEN `week_date` >= '2020-06-15' THEN `sales` ELSE 0 END) AS after_sales
    FROM filtered_sales
    GROUP BY `age_band`
),
demographic_data AS (
    SELECT 
        'demographic' AS attribute_type,
        `demographic` AS attribute_value,
        SUM(CASE WHEN `week_date` <= '2020-06-08' THEN `sales` ELSE 0 END) AS before_sales,
        SUM(CASE WHEN `week_date` >= '2020-06-15' THEN `sales` ELSE 0 END) AS after_sales
    FROM filtered_sales
    GROUP BY `demographic`
),
customer_type_data AS (
    SELECT 
        'customer_type' AS attribute_type,
        `customer_type` AS attribute_value,
        SUM(CASE WHEN `week_date` <= '2020-06-08' THEN `sales` ELSE 0 END) AS before_sales,
        SUM(CASE WHEN `week_date` >= '2020-06-15' THEN `sales` ELSE 0 END) AS after_sales
    FROM filtered_sales
    GROUP BY `customer_type`
),
all_data AS (
    SELECT * FROM region_data
    UNION ALL
    SELECT * FROM platform_data
    UNION ALL
    SELECT * FROM age_band_data
    UNION ALL
    SELECT * FROM demographic_data
    UNION ALL
    SELECT * FROM customer_type_data
),
pct_changes AS (
    SELECT 
        attribute_type,
        attribute_value,
        before_sales,
        after_sales,
        100.0 * (after_sales - before_sales) / before_sales AS pct_change
    FROM all_data
    WHERE before_sales > 0
),
avg_pct AS (
    SELECT 
        attribute_type,
        AVG(pct_change) AS avg_pct_change
    FROM pct_changes
    GROUP BY attribute_type
)
SELECT 
    attribute_type,
    avg_pct_change
FROM avg_pct
ORDER BY avg_pct_change
LIMIT 1