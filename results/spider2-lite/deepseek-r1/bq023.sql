SELECT 
  `bigquery-public-data.census_bureau_acs.censustract_2018_5yr`.`geo_id` AS census_tract,
  AVG(`bigquery-public-data.fec.indiv20`.`transaction_amt`) AS avg_donation_amount,
  `bigquery-public-data.census_bureau_acs.censustract_2018_5yr`.`median_income`
FROM 
  `bigquery-public-data.fec.indiv20`
JOIN 
  `bigquery-public-data.hud_zipcode_crosswalk.zipcode_to_census_tracts`
  ON SUBSTR(`bigquery-public-data.fec.indiv20`.`zip_code`, 1, 5) = `bigquery-public-data.hud_zipcode_crosswalk.zipcode_to_census_tracts`.`zip_code`
JOIN 
  `bigquery-public-data.census_bureau_acs.censustract_2018_5yr`
  ON `bigquery-public-data.hud_zipcode_crosswalk.zipcode_to_census_tracts`.`census_tract_geoid` = `bigquery-public-data.census_bureau_acs.censustract_2018_5yr`.`geo_id`
WHERE 
  `bigquery-public-data.fec.indiv20`.`state` = 'NY'
  AND `bigquery-public-data.census_bureau_acs.censustract_2018_5yr`.`geo_id` LIKE '36047%'
GROUP BY 
  `bigquery-public-data.census_bureau_acs.censustract_2018_5yr`.`geo_id`,
  `bigquery-public-data.census_bureau_acs.censustract_2018_5yr`.`median_income`