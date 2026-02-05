WITH amsterdam_stats AS (
  SELECT 
    b."CLASS",
    b."SUBCLASS",
    SUM(TRY_CAST(b."SURFACE_AREA_SQ_M" AS FLOAT)) AS total_surface_area,
    COUNT(*) AS total_buildings
  FROM "NETHERLANDS_OPEN_MAP_DATA"."NETHERLANDS"."V_BUILDING" b
  JOIN "NETHERLANDS_OPEN_MAP_DATA"."NETHERLANDS"."V_ADMINISTRATIVE" a
    ON ST_WITHIN(b."GEO_CORDINATES", a."GEO_CORDINATES")
  WHERE a."NAMES":"name"::STRING = 'Amsterdam'
    AND a."ADMIN_LEVEL" IN ('8', '10')
  GROUP BY b."CLASS", b."SUBCLASS"
),
rotterdam_stats AS (
  SELECT 
    b."CLASS",
    b."SUBCLASS",
    SUM(TRY_CAST(b."SURFACE_AREA_SQ_M" AS FLOAT)) AS total_surface_area,
    COUNT(*) AS total_buildings
  FROM "NETHERLANDS_OPEN_MAP_DATA"."NETHERLANDS"."V_BUILDING" b
  JOIN "NETHERLANDS_OPEN_MAP_DATA"."NETHERLANDS"."V_ADMINISTRATIVE" a
    ON ST_WITHIN(b."GEO_CORDINATES", a."GEO_CORDINATES")
  WHERE a."NAMES":"name"::STRING = 'Rotterdam'
    AND a."ADMIN_LEVEL" IN ('8', '10')
  GROUP BY b."CLASS", b."SUBCLASS"
)
SELECT 
  COALESCE(a."CLASS", r."CLASS") AS "CLASS",
  COALESCE(a."SUBCLASS", r."SUBCLASS") AS "SUBCLASS",
  COALESCE(a.total_surface_area, 0) AS total_surface_area_amsterdam,
  COALESCE(a.total_buildings, 0) AS total_buildings_amsterdam,
  COALESCE(r.total_surface_area, 0) AS total_surface_area_rotterdam,
  COALESCE(r.total_buildings, 0) AS total_buildings_rotterdam
FROM amsterdam_stats a
FULL OUTER JOIN rotterdam_stats r
  ON a."CLASS" = r."CLASS" AND a."SUBCLASS" = r."SUBCLASS"
ORDER BY COALESCE(a."CLASS", r."CLASS"), COALESCE(a."SUBCLASS", r."SUBCLASS")