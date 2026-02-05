WITH ny_cocs AS (
  SELECT DISTINCT `CoC_Number`, `CoC_Name`
  FROM `bigquery-public-data.sdoh_hud_pit_homelessness.hud_pit_by_coc`
  WHERE `CoC_Number` LIKE 'NY-%'
),
data_2012 AS (
  SELECT `CoC_Number`, `Homeless_Veterans` AS veterans_2012
  FROM `bigquery-public-data.sdoh_hud_pit_homelessness.hud_pit_by_coc`
  WHERE `Count_Year` = 2012
),
data_2018 AS (
  SELECT `CoC_Number`, `Homeless_Veterans` AS veterans_2018
  FROM `bigquery-public-data.sdoh_hud_pit_homelessness.hud_pit_by_coc`
  WHERE `Count_Year` = 2018
)
SELECT 
  ny.`CoC_Number`,
  ny.`CoC_Name`,
  COALESCE(d2018.`veterans_2018`, 0) - COALESCE(d2012.`veterans_2012`, 0) AS change_in_veterans
FROM ny_cocs ny
INNER JOIN data_2012 d2012 ON ny.`CoC_Number` = d2012.`CoC_Number`
INNER JOIN data_2018 d2018 ON ny.`CoC_Number` = d2018.`CoC_Number`
ORDER BY ny.`CoC_Number`