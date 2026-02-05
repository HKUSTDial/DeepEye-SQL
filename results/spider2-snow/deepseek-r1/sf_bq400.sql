SELECT
    t."route_id",
    t."trip_headsign",
    TO_CHAR(MIN(st1."departure_time"), 'HH24:MI:SS') AS earliest_departure,
    TO_CHAR(MAX(st2."arrival_time"), 'HH24:MI:SS') AS latest_arrival
FROM
    "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_TRANSIT_MUNI"."STOP_TIMES" st1
    INNER JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_TRANSIT_MUNI"."STOP_TIMES" st2
        ON st1."trip_id" = st2."trip_id"
    INNER JOIN "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_TRANSIT_MUNI"."TRIPS" t
        ON CAST(st1."trip_id" AS VARCHAR) = t."trip_id"
WHERE
    CAST(st1."stop_id" AS VARCHAR) = (SELECT "stop_id" FROM "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_TRANSIT_MUNI"."STOPS" WHERE "stop_name" = 'Clay St & Drumm St')
    AND CAST(st2."stop_id" AS VARCHAR) = (SELECT "stop_id" FROM "SAN_FRANCISCO_PLUS"."SAN_FRANCISCO_TRANSIT_MUNI"."STOPS" WHERE "stop_name" = 'Sacramento St & Davis St')
    AND st1."stop_sequence" < st2."stop_sequence"
GROUP BY
    t."route_id",
    t."trip_headsign"