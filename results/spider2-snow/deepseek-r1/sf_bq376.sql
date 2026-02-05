WITH station_counts AS (
   SELECT b."neighborhood", COUNT(DISTINCT s."station_id") AS station_count
   FROM "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_NEIGHBORHOODS"."BOUNDARIES" AS b
   INNER JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_BIKESHARE"."BIKESHARE_STATION_INFO" AS s
      ON ST_CONTAINS(TO_GEOGRAPHY(b."neighborhood_geom"), ST_MAKEPOINT(s."lon", s."lat"))
   GROUP BY b."neighborhood"
),
incident_counts AS (
   SELECT b."neighborhood", COUNT(DISTINCT i."unique_key") AS incident_count
   FROM "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_NEIGHBORHOODS"."BOUNDARIES" AS b
   INNER JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_SFPD_INCIDENTS"."SFPD_INCIDENTS" AS i
      ON ST_CONTAINS(TO_GEOGRAPHY(b."neighborhood_geom"), ST_MAKEPOINT(i."longitude", i."latitude"))
   GROUP BY b."neighborhood"
)
SELECT s."neighborhood", s.station_count, i.incident_count
FROM station_counts s
INNER JOIN incident_counts i ON s."neighborhood" = i."neighborhood"