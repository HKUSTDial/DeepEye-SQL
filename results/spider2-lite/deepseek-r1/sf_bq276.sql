WITH "ports_in_states" AS (
  SELECT 
    p."port_name",
    p."port_geom",
    s."state_name",
    s."state_geom"
  FROM "NOAA_PORTS"."GEO_INTERNATIONAL_PORTS"."WORLD_PORT_INDEX" p
  JOIN "NOAA_PORTS"."GEO_US_BOUNDARIES"."STATES" s
    ON ST_WITHIN(p."port_geom", s."state_geom")
  WHERE p."region_number" = '6585'
),
"filtered_storms" AS (
  SELECT 
    "sid",
    "name",
    "season",
    "usa_sshs",
    "usa_wind",
    "longitude",
    "latitude"
  FROM "NOAA_PORTS"."NOAA_HURRICANES"."HURRICANES"
  WHERE "basin" = 'NA'
    AND "usa_wind" >= 35
    AND "usa_sshs" >= 0
    AND "name" != 'NOT_NAMED'
),
"storm_points" AS (
  SELECT 
    "sid",
    "name",
    "season",
    "usa_sshs",
    "usa_wind",
    ST_MAKEPOINT("longitude", "latitude") AS "storm_point"
  FROM "filtered_storms"
),
"storm_points_in_states" AS (
  SELECT 
    sp."sid",
    sp."name",
    sp."season",
    sp."usa_sshs",
    sp."usa_wind",
    s."state_name"
  FROM "storm_points" sp
  JOIN "NOAA_PORTS"."GEO_US_BOUNDARIES"."STATES" s
    ON ST_WITHIN(sp."storm_point", s."state_geom")
),
"storms_per_state" AS (
  SELECT 
    "state_name",
    "sid",
    "name",
    "season",
    AVG("usa_sshs") AS "avg_sshs",
    AVG("usa_wind") AS "avg_wind"
  FROM "storm_points_in_states"
  GROUP BY "state_name", "sid", "name", "season"
),
"port_storms" AS (
  SELECT 
    pis."port_name",
    pis."state_name",
    pis."port_geom",
    pis."state_geom",
    sps."sid",
    sps."name" AS "storm_name",
    sps."season" AS "storm_year",
    sps."avg_sshs",
    sps."avg_wind"
  FROM "ports_in_states" pis
  JOIN "storms_per_state" sps ON pis."state_name" = sps."state_name"
)
SELECT 
  "port_name",
  "state_name",
  ARRAY_AGG(DISTINCT "storm_year") AS "distinct_years",
  COUNT(DISTINCT "sid") AS "total_distinct_storms",
  ARRAY_AGG(DISTINCT "storm_name") AS "distinct_storm_names",
  AVG("avg_sshs") AS "avg_storm_category",
  AVG("avg_wind") AS "avg_wind_speed",
  "port_geom",
  "state_geom"
FROM "port_storms"
GROUP BY "port_name", "state_name", "port_geom", "state_geom"