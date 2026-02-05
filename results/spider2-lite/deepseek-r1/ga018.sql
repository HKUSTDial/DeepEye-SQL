WITH categories AS (
  SELECT LOWER(category) AS category FROM UNNEST([
    'Accessories', 'Apparel', 'Brands', 'Campus Collection', 'Drinkware', 'Electronics',
    'Google Redesign', 'Lifestyle', 'Nest', 'New 2015 Logo', 'Notebooks Journals',
    'Office', 'Shop by Brand', 'Small Goods', 'Stationery', 'Wearables'
  ]) AS category
),
page_views AS (
  SELECT
    `user_pseudo_id`,
    (SELECT `value`.`int_value` FROM UNNEST(`event_params`) WHERE `key` = 'ga_session_id') AS `session_id`,
    `event_timestamp`,
    (SELECT `value`.`string_value` FROM UNNEST(`event_params`) WHERE `key` = 'page_location') AS `page_location`
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce`.`events_20210102`
  WHERE `event_name` = 'page_view'
    AND (SELECT `value`.`string_value` FROM UNNEST(`event_params`) WHERE `key` = 'page_location') IS NOT NULL
    AND (SELECT `value`.`int_value` FROM UNNEST(`event_params`) WHERE `key` = 'ga_session_id') IS NOT NULL
),
classified AS (
  SELECT
    `user_pseudo_id`,
    `session_id`,
    `event_timestamp`,
    `page_location`,
    SPLIT(`page_location`, '/') AS `segments`,
    ARRAY_LENGTH(SPLIT(`page_location`, '/')) AS `num_segments`
  FROM `page_views`
),
with_segments AS (
  SELECT
    `user_pseudo_id`,
    `session_id`,
    `event_timestamp`,
    `page_location`,
    `num_segments`,
    IF(`num_segments` >= 5, `segments`[OFFSET(3)], NULL) AS `seg4`,
    IF(`num_segments` >= 5, `segments`[OFFSET(4)], NULL) AS `seg5`,
    IF(`num_segments` >= 1, `segments`[OFFSET(`num_segments` - 1)], NULL) AS `last_seg`
  FROM `classified`
  WHERE `num_segments` >= 5
),
with_flags AS (
  SELECT
    `user_pseudo_id`,
    `session_id`,
    `event_timestamp`,
    `page_location`,
    CASE
      WHEN `num_segments` >= 5
           AND (`seg4` NOT LIKE '%+%' AND `seg5` NOT LIKE '%+%')
           AND (EXISTS (SELECT 1 FROM `categories` WHERE LOWER(REPLACE(`seg4`, '+', ' ')) LIKE CONCAT('%', `categories`.`category`, '%'))
                OR EXISTS (SELECT 1 FROM `categories` WHERE LOWER(REPLACE(`seg5`, '+', ' ')) LIKE CONCAT('%', `categories`.`category`, '%')))
      THEN 1 ELSE 0 END AS `is_plp`,
    CASE
      WHEN `num_segments` >= 5
           AND `last_seg` LIKE '%+%'
           AND (EXISTS (SELECT 1 FROM `categories` WHERE LOWER(REPLACE(`seg4`, '+', ' ')) LIKE CONCAT('%', `categories`.`category`, '%'))
                OR EXISTS (SELECT 1 FROM `categories` WHERE LOWER(REPLACE(`seg5`, '+', ' ')) LIKE CONCAT('%', `categories`.`category`, '%')))
      THEN 1 ELSE 0 END AS `is_pdp`
  FROM `with_segments`
),
session_events AS (
  SELECT
    *,
    MIN(CASE WHEN `is_pdp` = 1 THEN `event_timestamp` END)
      OVER (PARTITION BY `user_pseudo_id`, `session_id` ORDER BY `event_timestamp` ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS `next_pdp_timestamp`
  FROM `with_flags`
)
SELECT
  SUM(`is_plp`) AS `total_plp_views`,
  SUM(CASE WHEN `is_plp` = 1 AND `next_pdp_timestamp` IS NOT NULL THEN 1 ELSE 0 END) AS `plp_views_with_pdp`,
  SAFE_DIVIDE(SUM(CASE WHEN `is_plp` = 1 AND `next_pdp_timestamp` IS NOT NULL THEN 1 ELSE 0 END), SUM(`is_plp`)) * 100 AS `transition_percentage`
FROM `session_events`