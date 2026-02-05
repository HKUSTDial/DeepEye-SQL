SELECT DISTINCT "County_of_Residence", "Ave_Number_of_Prenatal_Wks"
FROM "SDOH"."SDOH_CDC_WONDER_NATALITY"."COUNTY_NATALITY"
WHERE "Year" = DATE '2018-01-01'
  AND "County_of_Residence_FIPS" IN (
    SELECT "geo_id"
    FROM "SDOH"."CENSUS_BUREAU_ACS"."COUNTY_2017_5YR"
    WHERE "geo_id" LIKE '55%'
      AND ("commute_45_59_mins" / NULLIF("employed_pop", 0)) > 0.05
  )