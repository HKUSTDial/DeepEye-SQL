WITH latest_released AS (
  SELECT
    "Name",
    "Version",
    "Links"
  FROM "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"
  WHERE "System" = 'NPM'
    AND "VersionInfo":IsRelease = true
    AND "Name" NOT LIKE '%@%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "Name" ORDER BY "VersionInfo":Ordinal DESC) = 1
),
package_dep_counts AS (
  SELECT
    lr."Name",
    lr."Version",
    lr."Links",
    COUNT(d."Dependency") AS dep_count
  FROM latest_released lr
  LEFT JOIN "DEPS_DEV_V1"."DEPS_DEV_V1"."DEPENDENCIES" d
    ON lr."Name" = d."Name"
    AND lr."Version" = d."Version"
    AND d."System" = 'NPM'
  GROUP BY lr."Name", lr."Version", lr."Links"
),
max_dep_package AS (
  SELECT
    "Name",
    "Version",
    "Links",
    dep_count
  FROM package_dep_counts
  ORDER BY dep_count DESC
  LIMIT 1
)
SELECT
  link.value:URL::STRING AS github_url
FROM max_dep_package,
LATERAL FLATTEN(INPUT => "Links") AS link
WHERE link.value:Label::STRING = 'SOURCE_REPO'
  AND link.value:URL::STRING LIKE '%github.com%'
LIMIT 1