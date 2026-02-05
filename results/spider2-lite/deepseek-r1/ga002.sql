WITH teebuyers AS (
  SELECT DISTINCT
    SUBSTR(event_date,1,6) AS month,
    user_pseudo_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  CROSS JOIN UNNEST(items) AS item
  WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
    AND event_date BETWEEN '20201101' AND '20210131'
    AND event_name = 'purchase'
    AND item.item_name = 'Google Red Speckled Tee'
),
other_purchases AS (
  SELECT
    SUBSTR(e.event_date,1,6) AS month,
    item.item_name,
    SUM(item.quantity) AS total_quantity
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
  CROSS JOIN UNNEST(items) AS item
  INNER JOIN teebuyers t
    ON t.user_pseudo_id = e.user_pseudo_id
    AND t.month = SUBSTR(e.event_date,1,6)
  WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
    AND e.event_date BETWEEN '20201101' AND '20210131'
    AND e.event_name = 'purchase'
    AND item.item_name != 'Google Red Speckled Tee'
  GROUP BY month, item.item_name
),
ranked AS (
  SELECT
    month,
    item_name,
    total_quantity,
    RANK() OVER (PARTITION BY month ORDER BY total_quantity DESC) AS rank
  FROM other_purchases
)
SELECT month, item_name, total_quantity
FROM ranked
WHERE rank = 1
ORDER BY month