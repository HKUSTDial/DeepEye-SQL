SELECT o.title, o.artist_display_name, o.medium, i.original_image_url
FROM `bigquery-public-data.the_met.objects` AS o
INNER JOIN `bigquery-public-data.the_met.images` AS i
ON o.object_id = i.object_id
WHERE o.department = 'Photographs'
AND o.object_name LIKE '%Photograph%'
AND o.object_end_date <= 1839
AND o.artist_display_name IS NOT NULL
AND o.artist_display_name != 'Unknown'