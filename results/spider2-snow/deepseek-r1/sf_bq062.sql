WITH flattened_licenses AS (
  SELECT 
    "System",
    "Name" AS package_name,
    f.value::STRING AS license
  FROM "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS",
  LATERAL FLATTEN(INPUT => "Licenses") f
),
license_counts AS (
  SELECT 
    "System",
    license,
    COUNT(DISTINCT package_name) AS package_count
  FROM flattened_licenses
  GROUP BY "System", license
),
ranked_licenses AS (
  SELECT 
    "System",
    license,
    package_count,
    RANK() OVER (PARTITION BY "System" ORDER BY package_count DESC) AS rank_num
  FROM license_counts
)
SELECT 
  "System",
  license,
  package_count
FROM ranked_licenses
WHERE rank_num = 1
ORDER BY "System"