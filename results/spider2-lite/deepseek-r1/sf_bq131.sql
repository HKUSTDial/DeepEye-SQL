WITH denmark AS (
  SELECT TO_GEOGRAPHY("geometry") AS geom
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES"
  WHERE "feature_type" = 'multipolygons'
    AND EXISTS (
      SELECT 1
      FROM LATERAL FLATTEN(INPUT => "all_tags") AS t
      WHERE t.value:"key"::STRING = 'wikidata'
        AND t.value:"value"::STRING = 'Q35'
    )
),
bus_stops AS (
  SELECT 
    p."osm_id",
    (SELECT t2.value:"value"::STRING
     FROM LATERAL FLATTEN(INPUT => p."all_tags") AS t2
     WHERE t2.value:"key"::STRING = 'network'
     LIMIT 1) AS network
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES_POINTS" AS p
  WHERE 
    EXISTS (
      SELECT 1
      FROM LATERAL FLATTEN(INPUT => p."all_tags") AS t3
      WHERE (t3.value:"key"::STRING = 'highway' AND t3.value:"value"::STRING = 'bus_stop')
         OR (t3.value:"key"::STRING = 'public_transport' AND t3.value:"value"::STRING = 'stop_position')
    )
    AND ST_WITHIN(TO_GEOGRAPHY(p."geometry"), (SELECT geom FROM denmark))
)
SELECT stop_count AS "number_of_bus_stops"
FROM (
  SELECT network, COUNT(*) AS stop_count
  FROM bus_stops
  WHERE network IS NOT NULL
  GROUP BY network
  ORDER BY stop_count DESC
  LIMIT 1
)