WITH "LatestReleaseVersions" AS (
    SELECT
        "Name",
        "Version",
        "VersionInfo":IsRelease::BOOLEAN AS "IsRelease",
        "VersionInfo":Ordinal::INT AS "Ordinal",
        ROW_NUMBER() OVER (PARTITION BY "Name" ORDER BY "VersionInfo":Ordinal::INT DESC) AS "rn"
    FROM "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONS"
    WHERE "System" = 'NPM'
        AND "VersionInfo":IsRelease::BOOLEAN = true
)
SELECT
    pv."Name" AS "PackageName",
    pv."Version" AS "LatestReleaseVersion",
    pr."StarsCount" AS "GithubStars"
FROM "LatestReleaseVersions" pv
INNER JOIN "DEPS_DEV_V1"."DEPS_DEV_V1"."PACKAGEVERSIONTOPROJECT" pvp
    ON pv."Name" = pvp."Name"
    AND pv."Version" = pvp."Version"
    AND pvp."System" = 'NPM'
    AND pvp."RelationType" = 'SOURCE_REPO_TYPE'
INNER JOIN "DEPS_DEV_V1"."DEPS_DEV_V1"."PROJECTS" pr
    ON pvp."ProjectName" = pr."Name"
    AND pr."Type" = 'GITHUB'
WHERE pv."rn" = 1
ORDER BY pr."StarsCount" DESC
LIMIT 8