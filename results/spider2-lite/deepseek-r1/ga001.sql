SELECT item.item_name AS other_product, SUM(item.quantity) AS total_quantity
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS events
CROSS JOIN UNNEST(items) AS item
WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
  AND event_name = 'purchase'
  AND ecommerce.transaction_id IN (
    SELECT DISTINCT ecommerce.transaction_id
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
      AND event_name = 'purchase'
      AND EXISTS (SELECT 1 FROM UNNEST(items) AS i WHERE i.item_name = 'Google Navy Speckled Tee')
  )
  AND item.item_name != 'Google Navy Speckled Tee'
GROUP BY item.item_name
ORDER BY total_quantity DESC
LIMIT 1