SELECT 
  CASE 
    WHEN `journal`.`title` IS NOT NULL THEN `journal`.`title`
    WHEN `proceedings_title`.`preferred` IS NOT NULL THEN `proceedings_title`.`preferred`
    WHEN `book_title`.`preferred` IS NOT NULL THEN `book_title`.`preferred`
    WHEN `book_series_title`.`preferred` IS NOT NULL THEN `book_series_title`.`preferred`
    ELSE NULL
  END AS venue_title
FROM `bigquery-public-data.dimensions_ai_covid19.publications` AS pubs
WHERE `pubs`.`date_inserted` >= TIMESTAMP('2021-01-01')
  AND EXISTS (
    SELECT 1 
    FROM UNNEST(`pubs`.`research_orgs`) AS org_id
    INNER JOIN `bigquery-public-data.dimensions_ai_covid19.grid` AS grid
      ON `grid`.`id` = org_id
    WHERE `grid`.`address`.`city` = 'Qianjiang'
  )