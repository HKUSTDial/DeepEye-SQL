WITH all_data AS (
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2005"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2006"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2007"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2008"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2009"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2010"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2011"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2012"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2013"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2014"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
  UNION ALL
  SELECT "latitude", "longitude", "year", "month", "day", "wind_speed"
  FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2015"
  WHERE "wind_speed" IS NOT NULL AND "year" BETWEEN 2005 AND 2015
)
SELECT "latitude", "longitude", "year", "month", "day", AVG("wind_speed") AS daily_avg_wind_speed
FROM all_data
GROUP BY "latitude", "longitude", "year", "month", "day"
ORDER BY daily_avg_wind_speed DESC
LIMIT 5