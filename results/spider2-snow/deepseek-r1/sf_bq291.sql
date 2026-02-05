WITH location_buffer AS (
  SELECT ST_BUFFER(ST_MAKEPOINT(51.5, 26.75), 5000) as buffer_geom
),
filtered_geographies AS (
  SELECT DISTINCT g.geography_polygon, g.creation_time, g.forecast
  FROM "NOAA_GLOBAL_FORECAST_SYSTEM"."NOAA_GLOBAL_FORECAST_SYSTEM"."NOAA_GFS0P25" g
  CROSS JOIN location_buffer lb
  WHERE ST_WITHIN(TO_GEOGRAPHY(g.geography_polygon), lb.buffer_geom)
    AND EXTRACT(YEAR FROM DATEADD('MICROSECOND', g.creation_time, '1970-01-01')) = 2019
    AND EXTRACT(MONTH FROM DATEADD('MICROSECOND', g.creation_time, '1970-01-01')) = 7
),
flattened_forecasts AS (
  SELECT 
    DATEADD('DAY', 1, DATEADD('MICROSECOND', fg.creation_time, '1970-01-01')) as forecast_date,
    ff.value as forecast_data
  FROM filtered_geographies fg,
  LATERAL FLATTEN(INPUT => fg.forecast) ff
  WHERE DATE(ff.value:valid_time) = DATEADD('DAY', 1, DATEADD('MICROSECOND', fg.creation_time, '1970-01-01'))
),
temperature_conversions AS (
  SELECT 
    forecast_date,
    (forecast_data:tmp2m::FLOAT - 273.15) * 9/5 + 32 as temp_f,
    forecast_data:prate::FLOAT as precip_rate,
    forecast_data:tcdc::FLOAT as cloud_cover,
    EXTRACT(HOUR FROM forecast_data:valid_time) as hour
  FROM flattened_forecasts
)
SELECT 
  forecast_date,
  MAX(temp_f) as max_temperature_f,
  MIN(temp_f) as min_temperature_f,
  AVG(temp_f) as avg_temperature_f,
  SUM(precip_rate * 3600) as total_precipitation_mm,
  AVG(CASE WHEN hour BETWEEN 10 AND 17 THEN cloud_cover END) as avg_cloud_cover_10am_to_5pm,
  CASE 
    WHEN AVG(temp_f) < 32 THEN SUM(precip_rate * 3600)
    ELSE 0 
  END as total_snowfall_mm,
  CASE 
    WHEN AVG(temp_f) >= 32 THEN SUM(precip_rate * 3600)
    ELSE 0 
  END as total_rainfall_mm
FROM temperature_conversions
GROUP BY forecast_date
ORDER BY forecast_date