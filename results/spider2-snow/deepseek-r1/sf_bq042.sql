WITH all_years_data AS (
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2011"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2012"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2013"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2014"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2015"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2016"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2017"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2018"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2019"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
  
  UNION ALL
  
  SELECT "year", "temp", TRY_CAST("wdsp" AS FLOAT) AS "wdsp_float", "prcp"
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2020"
  WHERE "stn" = '725030' AND "mo" = '06' AND "da" = '12' 
    AND "temp" != 9999.9 AND "wdsp" != '999.9' AND "prcp" != 99.99
)
SELECT "year", AVG("temp") AS "avg_temperature", AVG("wdsp_float") AS "avg_wind_speed", AVG("prcp") AS "precipitation"
FROM all_years_data
GROUP BY "year"
ORDER BY "year"