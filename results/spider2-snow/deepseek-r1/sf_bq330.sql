WITH colorado_zips AS (
  SELECT "zip_code", TO_GEOGRAPHY("zip_code_geom") AS zip_geom
  FROM "FDA"."GEO_US_BOUNDARIES"."ZIP_CODES"
  WHERE "state_code" = 'CO'
), colorado_blockgroups AS (
  SELECT "geo_id", TO_GEOGRAPHY("blockgroup_geom") AS bg_geom, "area_land_meters"
  FROM "FDA"."GEO_CENSUS_BLOCKGROUPS"."US_BLOCKGROUPS_NATIONAL"
  WHERE "state_fips_code" = '08'
), intersections AS (
  SELECT 
    cz."zip_code",
    cb."geo_id",
    cb."area_land_meters",
    ST_AREA(ST_INTERSECTION(cz.zip_geom, cb.bg_geom)) AS intersection_area
  FROM colorado_zips cz
  JOIN colorado_blockgroups cb
    ON ST_INTERSECTS(cz.zip_geom, cb.bg_geom)
  WHERE intersection_area > 0 AND cb."area_land_meters" > 0
), bank_counts AS (
  SELECT "zip_code", COUNT(*) AS bank_count
  FROM "FDA"."FDIC_BANKS"."LOCATIONS"
  WHERE "state" = 'CO'
  GROUP BY "zip_code"
), estimated_per_bg AS (
  SELECT 
    i."zip_code",
    i."geo_id",
    bc.bank_count * (i.intersection_area / i."area_land_meters") AS estimated_banks
  FROM intersections i
  JOIN bank_counts bc ON i."zip_code" = bc."zip_code"
), max_per_zip AS (
  SELECT 
    "zip_code",
    MAX(estimated_banks) AS max_estimated_banks_per_bg
  FROM estimated_per_bg
  GROUP BY "zip_code"
)
SELECT "zip_code"
FROM max_per_zip
ORDER BY max_estimated_banks_per_bg DESC
LIMIT 1