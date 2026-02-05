WITH tract_populations AS (
    SELECT 
        g."StateCountyTractID",
        SUM(f."CensusValue") AS tract_population
    FROM "CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE"."PUBLIC"."Dim_CensusGeography" g
    INNER JOIN "CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE"."PUBLIC"."Fact_CensusValues_ACS2021" f 
        ON g."BlockGroupID" = f."BlockGroupID"
    WHERE g."StateAbbrev" = 'NY'
        AND f."MetricID" = 'B01003_001E'
    GROUP BY g."StateCountyTractID"
)
SELECT 
    g."BlockGroupID",
    f."CensusValue" AS census_value,
    g."StateCountyTractID",
    tp.tract_population AS total_tract_population,
    f."CensusValue" / NULLIF(tp.tract_population, 0) AS population_ratio
FROM "CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE"."PUBLIC"."Dim_CensusGeography" g
INNER JOIN "CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE"."PUBLIC"."Fact_CensusValues_ACS2021" f 
    ON g."BlockGroupID" = f."BlockGroupID"
INNER JOIN tract_populations tp 
    ON g."StateCountyTractID" = tp."StateCountyTractID"
WHERE g."StateAbbrev" = 'NY'
    AND f."MetricID" = 'B01003_001E'
ORDER BY g."StateCountyTractID", g."BlockGroupID"