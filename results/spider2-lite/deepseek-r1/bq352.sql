SELECT 
    n.`County_of_Residence_FIPS`,
    n.`County_of_Residence`,
    n.`Ave_Number_of_Prenatal_Wks`
FROM 
    `bigquery-public-data.sdoh_cdc_wonder_natality.county_natality` AS n
INNER JOIN 
    `bigquery-public-data.census_bureau_acs.county_2017_1yr` AS c
    ON n.`County_of_Residence_FIPS` = c.`geo_id`
WHERE 
    c.`geo_id` LIKE '55%'
    AND (c.`commute_45_59_mins` / c.`employed_pop`) * 100 > 5
    AND c.`do_date` = '2017'
    AND EXTRACT(YEAR FROM n.`Year`) = 2018