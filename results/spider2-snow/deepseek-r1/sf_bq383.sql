WITH yearly_data AS (
  SELECT 
    EXTRACT(YEAR FROM "date") as year,
    "element",
    "value"/10.0 as adjusted_value
  FROM "GHCN_D"."GHCN_D"."GHCND_2013"
  WHERE "id" = 'USW00094846'
    AND "element" IN ('PRCP', 'TMIN', 'TMAX')
    AND EXTRACT(MONTH FROM "date") = 12
    AND EXTRACT(DAY FROM "date") >= 17
    AND "qflag" IS NULL
    AND "value" IS NOT NULL
  UNION ALL
  SELECT 
    EXTRACT(YEAR FROM "date") as year,
    "element",
    "value"/10.0 as adjusted_value
  FROM "GHCN_D"."GHCN_D"."GHCND_2014"
  WHERE "id" = 'USW00094846'
    AND "element" IN ('PRCP', 'TMIN', 'TMAX')
    AND EXTRACT(MONTH FROM "date") = 12
    AND EXTRACT(DAY FROM "date") >= 17
    AND "qflag" IS NULL
    AND "value" IS NOT NULL
  UNION ALL
  SELECT 
    EXTRACT(YEAR FROM "date") as year,
    "element",
    "value"/10.0 as adjusted_value
  FROM "GHCN_D"."GHCN_D"."GHCND_2015"
  WHERE "id" = 'USW00094846'
    AND "element" IN ('PRCP', 'TMIN', 'TMAX')
    AND EXTRACT(MONTH FROM "date") = 12
    AND EXTRACT(DAY FROM "date") >= 17
    AND "qflag" IS NULL
    AND "value" IS NOT NULL
  UNION ALL
  SELECT 
    EXTRACT(YEAR FROM "date") as year,
    "element",
    "value"/10.0 as adjusted_value
  FROM "GHCN_D"."GHCN_D"."GHCND_2016"
  WHERE "id" = 'USW00094846'
    AND "element" IN ('PRCP', 'TMIN', 'TMAX')
    AND EXTRACT(MONTH FROM "date") = 12
    AND EXTRACT(DAY FROM "date") >= 17
    AND "qflag" IS NULL
    AND "value" IS NOT NULL
)
SELECT 
  year,
  MAX(CASE WHEN "element" = 'PRCP' THEN adjusted_value END) as highest_precipitation_mm,
  MIN(CASE WHEN "element" = 'TMIN' THEN adjusted_value END) as minimum_temperature_c,
  MAX(CASE WHEN "element" = 'TMAX' THEN adjusted_value END) as maximum_temperature_c
FROM yearly_data
GROUP BY year
ORDER BY year