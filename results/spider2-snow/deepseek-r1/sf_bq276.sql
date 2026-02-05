WITH ports AS (
  SELECT 
    p."port_name",
    p."port_geom",
    s."state_name"
  FROM "NOAA_PORTS"."GEO_INTERNATIONAL_PORTS"."WORLD_PORT_INDEX" p
  JOIN "NOAA_PORTS"."GEO_US_BOUNDARIES"."STATES" s
    ON ST_WITHIN(p."port_geom", s."state_geom")
  WHERE p."region_number" = '6585'
),
storms AS (
  SELECT 
    "sid",
    "season",
    "name",
    "usa_sshs",
    "usa_wind",
    "longitude",
    "latitude",
    COALESCE("usa_r34_ne", 0) AS "usa_r34_ne",
    COALESCE("usa_r34_se", 0) AS "usa_r34_se",
    COALESCE("usa_r34_sw", 0) AS "usa_r34_sw",
    COALESCE("usa_r34_nw", 0) AS "usa_r34_nw"
  FROM "NOAA_PORTS"."NOAA_HURRICANES"."HURRICANES"
  WHERE "basin" = 'NA'
    AND "name" != 'NOT_NAMED'
    AND "usa_wind" >= 35
    AND "usa_sshs" >= 0
),
affected AS (
  SELECT 
    ports."port_name",
    ports."state_name",
    ports."port_geom",
    storms."sid",
    storms."season",
    storms."name",
    storms."usa_sshs",
    storms."usa_wind",
    ST_MAKEPOINT(storms."longitude", storms."latitude") AS "storm_point"
  FROM ports
  JOIN storms
    ON ST_DISTANCE(ports."port_geom", ST_MAKEPOINT(storms."longitude", storms."latitude")) <= 
       (GREATEST(storms."usa_r34_ne", storms."usa_r34_se", storms."usa_r34_sw", storms."usa_r34_nw") * 1852)
)
SELECT 
  "port_name",
  "state_name",
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT "season"), ', ') AS "distinct_years",
  COUNT(DISTINCT "sid") AS "total_distinct_storms",
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT "name"), ', ') AS "distinct_storm_names",
  AVG("usa_sshs") AS "average_storm_category",
  AVG("usa_wind") AS "average_wind_speed",
  "port_geom",
  ST_COLLECT("storm_point") AS "tropical_storm_areas_geom"
FROM affected
GROUP BY "port_name", "state_name", "port_geom"