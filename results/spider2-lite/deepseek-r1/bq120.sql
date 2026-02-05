SELECT
  snap.`GeoDesc` AS region,
  snap.`SNAP_All_Participation_Households` AS snap_households,
  (acs.`income_less_10000` + acs.`income_10000_14999` + acs.`income_15000_19999`) AS low_income_households,
  (acs.`income_less_10000` + acs.`income_10000_14999` + acs.`income_15000_19999`) / snap.`SNAP_All_Participation_Households` AS ratio
FROM
  `bigquery-public-data.sdoh_snap_enrollment.snap_enrollment` AS snap
INNER JOIN
  `bigquery-public-data.census_bureau_acs.county_2017_5yr` AS acs
  ON snap.`FIPS` = acs.`geo_id`
WHERE
  snap.`Date` = DATE '2017-01-01'
  AND snap.`SNAP_All_Participation_Households` > 0
ORDER BY
  snap.`SNAP_All_Participation_Households` DESC
LIMIT 10