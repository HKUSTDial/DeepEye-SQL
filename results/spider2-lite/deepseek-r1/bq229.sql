SELECT
  COUNT(DISTINCT CASE 
    WHEN l.label_name = '/m/01yrx' AND l.confidence = 1 
    THEN i.original_url 
  END) AS cat_url_count,
  COUNT(DISTINCT CASE 
    WHEN l.label_name IS NULL 
    THEN i.original_url 
  END) AS other_url_count
FROM `bigquery-public-data.open_images.images` AS i
LEFT JOIN `bigquery-public-data.open_images.labels` AS l
  ON i.image_id = l.image_id 
  AND l.label_name = '/m/01yrx'