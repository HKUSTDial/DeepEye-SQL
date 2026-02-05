SELECT
    snap."FIPS",
    snap."GeoDesc",
    snap."SNAP_All_Participation_Households",
    (acs."income_less_10000" + acs."income_10000_14999" + acs."income_15000_19999") AS "low_income_households",
    "low_income_households" / snap."SNAP_All_Participation_Households" AS "ratio"
FROM "SDOH"."SDOH_SNAP_ENROLLMENT"."SNAP_ENROLLMENT" AS snap
JOIN "SDOH"."CENSUS_BUREAU_ACS"."COUNTY_2017_5YR" AS acs
    ON snap."FIPS" = acs."geo_id"
WHERE snap."Date" = '2017-01-01'
    AND snap."SNAP_All_Participation_Households" > 0
ORDER BY snap."SNAP_All_Participation_Households" DESC
LIMIT 10