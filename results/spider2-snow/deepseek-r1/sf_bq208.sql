WITH stations_near AS (
  SELECT "usaf", "wban", "name", "lat", "lon"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."STATIONS"
  WHERE ST_DWITHIN(ST_MAKEPOINT("lon", "lat"), ST_MAKEPOINT(-73.764, 41.197), 32186.9)
),
valid_obs AS (
  SELECT "stn", "wban", COUNT(*) AS "obs_count"
  FROM (
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2011" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2012" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2013" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2014" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2015" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2016" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2017" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2018" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2019" WHERE "temp" != 9999.9
    UNION ALL
    SELECT "stn", "wban" FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2020" WHERE "temp" != 9999.9
  ) AS all_years
  GROUP BY "stn", "wban"
)
SELECT s."usaf", s."wban", s."name", s."lat", s."lon", COALESCE(v."obs_count", 0) AS "valid_obs_count"
FROM stations_near s
LEFT JOIN valid_obs v ON s."usaf" = v."stn" AND s."wban" = v."wban"
ORDER BY s."usaf", s."wban"