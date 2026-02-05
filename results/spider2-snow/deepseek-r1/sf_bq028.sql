WITH LatestReleaseVersions AS (
    SELECT
        "System",
        "Name",
        "Version",
        "VersionInfo":"Ordinal" AS Ordinal
    FROM
        "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"
    WHERE
        "System" = 'NPM'
        AND "VersionInfo":"IsRelease" = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "Name" ORDER BY "VersionInfo":"Ordinal" DESC) = 1
),
PackageToRepo AS (
    SELECT
        "System",
        "Name",
        "Version",
        "ProjectName"
    FROM
        "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONTOPROJECT"
    WHERE
        "System" = 'NPM'
        AND "RelationType" = 'SOURCE_REPO_TYPE'
        AND "ProjectType" = 'GITHUB'
),
PackageStars AS (
    SELECT
        lrv."Name" AS PackageName,
        lrv."Version",
        p."StarsCount"
    FROM
        LatestReleaseVersions lrv
        INNER JOIN PackageToRepo ptr ON lrv."System" = ptr."System"
            AND lrv."Name" = ptr."Name"
            AND lrv."Version" = ptr."Version"
        INNER JOIN "DEPS_DEV_V1"."DEPS_DEV_V1"."PROJECTS" p ON ptr."ProjectName" = p."Name" AND p."Type" = 'GITHUB'
)
SELECT
    PackageName,
    "Version",
    "StarsCount"
FROM
    PackageStars
ORDER BY
    "StarsCount" DESC
LIMIT 8