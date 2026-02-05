WITH released_versions_with_github AS (
  SELECT DISTINCT
    "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."System",
    "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."Name",
    "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."Version",
    "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."Links",
    "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."VersionInfo":"Ordinal" AS "Ordinal"
  FROM "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS",
  LATERAL FLATTEN(INPUT => "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."Links") f
  WHERE "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."System" = 'NPM'
    AND "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."VersionInfo":"IsRelease" = true
    AND "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"."Name" NOT LIKE '%@%'
    AND f.value:"Label"::STRING = 'SOURCE_REPO'
    AND f.value:"URL"::STRING LIKE '%github.com%'
),
latest_released_per_package AS (
  SELECT
    "System",
    "Name",
    "Version",
    "Links",
    "Ordinal"
  FROM released_versions_with_github
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "Name" ORDER BY "Ordinal" DESC) = 1
),
package_dependency_counts AS (
  SELECT
    l."System",
    l."Name",
    l."Version",
    l."Links",
    COUNT(d."Dependency") AS "dep_count"
  FROM latest_released_per_package l
  LEFT JOIN "DEPS_DEV_V1"."DEPS_DEV_V1"."DEPENDENCIES" d
    ON l."System" = d."System"
    AND l."Name" = d."Name"
    AND l."Version" = d."Version"
  GROUP BY l."System", l."Name", l."Version", l."Links"
),
max_dep_count AS (
  SELECT MAX("dep_count") AS "max_count" FROM package_dependency_counts
),
top_package AS (
  SELECT
    p."System",
    p."Name",
    p."Version",
    p."Links",
    p."dep_count"
  FROM package_dependency_counts p
  INNER JOIN max_dep_count m ON p."dep_count" = m."max_count"
  QUALIFY ROW_NUMBER() OVER (ORDER BY p."Name") = 1
),
github_urls AS (
  SELECT
    f.value:"URL"::STRING AS "url"
  FROM top_package,
  LATERAL FLATTEN(INPUT => top_package."Links") f
  WHERE f.value:"Label"::STRING = 'SOURCE_REPO'
    AND f.value:"URL"::STRING LIKE '%github.com%'
  QUALIFY ROW_NUMBER() OVER (ORDER BY f.value:"URL"::STRING) = 1
)
SELECT "url" AS "github_url" FROM github_urls