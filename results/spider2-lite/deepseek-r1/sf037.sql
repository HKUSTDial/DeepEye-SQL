WITH "home_depot" AS (
  SELECT
    hd."POI_ID" AS "hd_poi_id",
    hd."POI_NAME" AS "hd_name",
    hd_addr."LATITUDE" AS "hd_lat",
    hd_addr."LONGITUDE" AS "hd_lon"
  FROM "US_REAL_ESTATE"."CYBERSYN"."POINT_OF_INTEREST_INDEX" hd
  INNER JOIN "US_REAL_ESTATE"."CYBERSYN"."POINT_OF_INTEREST_ADDRESSES_RELATIONSHIPS" hd_rel
    ON hd."POI_ID" = hd_rel."POI_ID"
  INNER JOIN "US_REAL_ESTATE"."CYBERSYN"."US_ADDRESSES" hd_addr
    ON hd_rel."ADDRESS_ID" = hd_addr."ADDRESS_ID"
  WHERE hd."POI_NAME" = 'The Home Depot'
), "lowes" AS (
  SELECT
    lowes."POI_ID" AS "lowes_poi_id",
    lowes."POI_NAME" AS "lowes_name",
    lowes_addr."LATITUDE" AS "lowes_lat",
    lowes_addr."LONGITUDE" AS "lowes_lon"
  FROM "US_REAL_ESTATE"."CYBERSYN"."POINT_OF_INTEREST_INDEX" lowes
  INNER JOIN "US_REAL_ESTATE"."CYBERSYN"."POINT_OF_INTEREST_ADDRESSES_RELATIONSHIPS" lowes_rel
    ON lowes."POI_ID" = lowes_rel."POI_ID"
  INNER JOIN "US_REAL_ESTATE"."CYBERSYN"."US_ADDRESSES" lowes_addr
    ON lowes_rel."ADDRESS_ID" = lowes_addr."ADDRESS_ID"
  WHERE lowes."POI_NAME" = 'Lowe''s Home Improvement'
)
SELECT
  "hd_poi_id",
  "hd_name",
  "lowes_poi_id",
  "lowes_name",
  ST_DISTANCE(
    TO_GEOGRAPHY(ST_MAKEPOINT("hd_lon", "hd_lat")),
    TO_GEOGRAPHY(ST_MAKEPOINT("lowes_lon", "lowes_lat"))
  ) / 1609 AS "distance_miles"
FROM "home_depot"
CROSS JOIN "lowes"
QUALIFY ROW_NUMBER() OVER (PARTITION BY "hd_poi_id" ORDER BY "distance_miles") = 1
ORDER BY "hd_poi_id"