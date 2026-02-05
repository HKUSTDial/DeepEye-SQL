SELECT DISTINCT o.object_id, o.title, FORMAT_DATE('%Y-%m-%d', DATE(o.metadata_date)) AS formatted_metadata_date
FROM `bigquery-public-data.the_met.objects` o
INNER JOIN `bigquery-public-data.the_met.vision_api_data` v ON o.object_id = v.object_id
WHERE o.department = 'The Libraries'
  AND CONTAINS_SUBSTR(o.title, 'book')
  AND EXISTS (SELECT 1 FROM UNNEST(v.cropHintsAnnotation.cropHints) AS hint WHERE hint.confidence > 0.5)