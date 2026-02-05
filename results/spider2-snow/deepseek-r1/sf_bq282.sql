SELECT
  "district"
FROM (
  SELECT
    s_start."council_district" AS "district",
    COUNT(*) AS "trip_count"
  FROM
    "AUSTIN"."AUSTIN_BIKESHARE"."BIKESHARE_TRIPS" AS t
  INNER JOIN
    "AUSTIN"."AUSTIN_BIKESHARE"."BIKESHARE_STATIONS" AS s_start
    ON t."start_station_id" = s_start."station_id"
  INNER JOIN
    "AUSTIN"."AUSTIN_BIKESHARE"."BIKESHARE_STATIONS" AS s_end
    ON TRY_CAST(t."end_station_id" AS NUMBER) = s_end."station_id"
  WHERE
    t."start_station_id" != TRY_CAST(t."end_station_id" AS NUMBER)
    AND s_start."council_district" = s_end."council_district"
    AND s_start."status" != 'closed'
    AND s_end."status" != 'closed'
  GROUP BY
    s_start."council_district"
) counts
ORDER BY
  "trip_count" DESC
LIMIT 1