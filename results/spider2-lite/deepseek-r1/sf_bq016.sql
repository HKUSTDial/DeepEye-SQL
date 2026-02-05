WITH "HighestReleaseVersions" AS (
  SELECT
    "System",
    "Name",
    "Version",
    "VersionInfo":"Ordinal"::INTEGER AS "Ordinal"
  FROM (
    SELECT
      "System",
      "Name",
      "Version",
      "VersionInfo",
      ROW_NUMBER() OVER (PARTITION BY "Name" ORDER BY "VersionInfo":"Ordinal"::INTEGER DESC) AS "rn"
    FROM "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"
    WHERE "System" = 'NPM'
      AND "VersionInfo":"IsRelease"::BOOLEAN = true
  ) t
  WHERE "rn" = 1
)
SELECT
  "Dependency":"Name"::TEXT AS "DependencyName",
  "Dependency":"Version"::TEXT AS "DependencyVersion",
  COUNT(*) AS "Frequency"
FROM "HighestReleaseVersions" hrv
INNER JOIN "DEPS_DEV_V1"."DEPS_DEV_V1"."DEPENDENCIES" d
  ON hrv."System" = d."System"
  AND hrv."Name" = d."Name"
  AND hrv."Version" = d."Version"
WHERE d."System" = 'NPM'
GROUP BY "DependencyName", "DependencyVersion"
ORDER BY "Frequency" DESC
LIMIT 1