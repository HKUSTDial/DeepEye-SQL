WITH highest_release AS (
  SELECT "System", "Name", "Version"
  FROM "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"
  WHERE "System" = 'NPM' AND "VersionInfo":"IsRelease"::BOOLEAN = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "Name" ORDER BY "VersionInfo":"Ordinal"::INTEGER DESC) = 1
),
dependencies_of_highest AS (
  SELECT 
    d."Dependency":"Name"::TEXT AS DepName,
    d."Dependency":"Version"::TEXT AS DepVersion
  FROM highest_release hr
  INNER JOIN "DEPS_DEV_V1"."DEPS_DEV_V1"."DEPENDENCIES" d
    ON hr."System" = d."System" AND hr."Name" = d."Name" AND hr."Version" = d."Version"
  WHERE d."System" = 'NPM'
)
SELECT DepName, DepVersion, COUNT(*) AS frequency
FROM dependencies_of_highest
GROUP BY DepName, DepVersion
ORDER BY frequency DESC
LIMIT 1