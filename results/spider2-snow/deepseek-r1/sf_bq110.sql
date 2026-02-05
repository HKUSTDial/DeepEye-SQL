SELECT
  "CoC_Number",
  "CoC_Name",
  MAX(CASE WHEN "Count_Year" = 2012 THEN "Homeless_Veterans" END) AS homeless_veterans_2012,
  MAX(CASE WHEN "Count_Year" = 2018 THEN "Homeless_Veterans" END) AS homeless_veterans_2018,
  homeless_veterans_2018 - homeless_veterans_2012 AS change
FROM "SDOH"."SDOH_HUD_PIT_HOMELESSNESS"."HUD_PIT_BY_COC"
WHERE "CoC_Number" LIKE 'NY-%'
  AND "Count_Year" IN (2012, 2018)
GROUP BY "CoC_Number", "CoC_Name"
HAVING homeless_veterans_2012 IS NOT NULL AND homeless_veterans_2018 IS NOT NULL