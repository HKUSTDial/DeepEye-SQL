WITH filtered_creations AS (
  SELECT 
    TO_GEOGRAPHY("geography") AS location_geog,
    "creation_time",
    "forecast"
  FROM "NOAA_GLOBAL_FORECAST_SYSTEM"."NOAA_GLOBAL_FORECAST_SYSTEM"."NOAA_GFS0P25"
  WHERE TO_TIMESTAMP("creation_time" / 1000000) >= '2019-07-01'
    AND TO_TIMESTAMP("creation_time" / 1000000) < '2019-08-01'
    AND ST_DWITHIN(TO_GEOGRAPHY("geography"), ST_POINT(51.5, 26.75), 5000)
),
flattened_forecasts AS (
  SELECT 
    fc.location_geog,
    TO_TIMESTAMP(fc."creation_time" / 1000000) AS creation_ts,
    f.value AS forecast_step
  FROM filtered_creations fc,
  LATERAL FLATTEN(input => fc."forecast") f
),
extracted_forecasts AS (
  SELECT 
    ff.location_geog,
    ff.creation_ts,
    ff.forecast_step:"valid_time"::TIMESTAMP_NTZ AS valid_time,
    ff.forecast_step:"temperature"::FLOAT AS temperature_k,
    ff.forecast_step:"precipitation"::FLOAT AS precipitation,
    ff.forecast_step:"cloud_cover"::FLOAT AS cloud_cover
  FROM flattened_forecasts ff
  WHERE DATE(ff.forecast_step:"valid_time"::TIMESTAMP_NTZ) = DATEADD(day, 1, DATE(ff.creation_ts))
),
converted_forecasts AS (
  SELECT 
    ef.location_geog,
    ef.creation_ts,
    ef.valid_time,
    ef.precipitation,
    ef.cloud_cover,
    (ef.temperature_k - 273.15) * 9/5 + 32 AS temp_f,
    DATE(ef.valid_time) AS forecast_date,
    EXTRACT(HOUR FROM ef.valid_time) AS hour
  FROM extracted_forecasts ef
),
daily_aggregates AS (
  SELECT 
    forecast_date,
    MAX(temp_f) AS max_temperature_f,
    MIN(temp_f) AS min_temperature_f,
    AVG(temp_f) AS avg_temperature_f,
    SUM(precipitation) AS total_precipitation,
    AVG(CASE WHEN hour BETWEEN 10 AND 17 THEN cloud_cover END) AS avg_cloud_cover
  FROM converted_forecasts
  GROUP BY forecast_date
)
SELECT 
  forecast_date,
  max_temperature_f,
  min_temperature_f,
  avg_temperature_f,
  total_precipitation,
  avg_cloud_cover,
  CASE WHEN avg_temperature_f < 32 THEN total_precipitation ELSE 0 END AS total_snowfall,
  CASE WHEN avg_temperature_f >= 32 THEN total_precipitation ELSE 0 END AS total_rainfall
FROM daily_aggregates
ORDER BY forecast_date