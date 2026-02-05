WITH license_counts AS (
  SELECT 
    "System", 
    licenses_flat.value::STRING as license_value, 
    COUNT(DISTINCT "Name") as package_count
  FROM 
    "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS",
    LATERAL FLATTEN(INPUT => PARSE_JSON("Licenses")) AS licenses_flat
  GROUP BY 
    "System", 
    license_value
)
SELECT 
  "System",
  license_value,
  package_count
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY "System" ORDER BY package_count DESC) as rn
  FROM license_counts
) 
WHERE rn = 1