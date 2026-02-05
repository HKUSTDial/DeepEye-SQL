WITH eligible_ports AS (
  SELECT p."index_number", p."port_name", TO_GEOGRAPHY(p."port_geom") AS port_geog
  FROM "NOAA_PORTS"."GEO_INTERNATIONAL_PORTS"."WORLD_PORT_INDEX" p
  JOIN "NOAA_PORTS"."GEO_US_BOUNDARIES"."STATES" s
    ON ST_WITHIN(TO_GEOGRAPHY(p."port_geom"), TO_GEOGRAPHY(s."state_geom"))
  WHERE p."region_number" = '6585'
), hurricane_points AS (
  SELECT 
    h."sid",
    h."iso_time",
    h."latitude",
    h."longitude",
    GREATEST(h."usa_r34_ne", h."usa_r34_se", h."usa_r34_sw", h."usa_r34_nw") AS max_radius_nm,
    ST_MAKEPOINT(h."longitude", h."latitude") AS hurricane_geog
  FROM "NOAA_PORTS"."NOAA_HURRICANES"."HURRICANES" h
  WHERE h."basin" = 'NA'
    AND h."usa_wind" >= 35
    AND h."name" != 'NOT_NAMED'
    AND h."usa_r34_ne" IS NOT NULL
    AND h."usa_r34_se" IS NOT NULL
    AND h."usa_r34_sw" IS NOT NULL
    AND h."usa_r34_nw" IS NOT NULL
), port_hurricane_matches AS (
  SELECT 
    ep."index_number",
    ep."port_name",
    COUNT(*) AS occurrence_count
  FROM eligible_ports ep
  JOIN hurricane_points hp
    ON ST_DWITHIN(ep.port_geog, hp.hurricane_geog, hp.max_radius_nm * 1852)
  GROUP BY ep."index_number", ep."port_name"
)
SELECT "port_name"
FROM port_hurricane_matches
ORDER BY occurrence_count DESC
LIMIT 1