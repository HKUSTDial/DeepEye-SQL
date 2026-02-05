SELECT
    t."bike_number",
    SUM(ST_DISTANCE(ST_MAKEPOINT(s1."longitude", s1."latitude"), ST_MAKEPOINT(s2."longitude", s2."latitude"))) AS "total_distance_meters"
FROM
    "SAN_FRANCISCO"."SAN_FRANCISCO"."BIKESHARE_TRIPS" t
    JOIN "SAN_FRANCISCO"."SAN_FRANCISCO"."BIKESHARE_STATIONS" s1 ON t."start_station_id" = s1."station_id"
    JOIN "SAN_FRANCISCO"."SAN_FRANCISCO"."BIKESHARE_STATIONS" s2 ON t."end_station_id" = s2."station_id"
GROUP BY
    t."bike_number"