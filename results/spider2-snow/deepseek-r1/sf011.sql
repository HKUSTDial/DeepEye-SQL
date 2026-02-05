WITH block_group_pop AS (
  SELECT
    d."BlockGroupID",
    d."StateCountyTractID",
    f."CensusValue" AS block_group_population
  FROM "CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE"."PUBLIC"."Dim_CensusGeography" d
  INNER JOIN "CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE"."PUBLIC"."Fact_CensusValues_ACS2021" f
    ON d."BlockGroupID" = f."BlockGroupID"
  WHERE d."StateName" = 'New York'
    AND f."MetricID" = 'B01003_001E'
)
SELECT
  "BlockGroupID",
  block_group_population AS "CensusValue",
  "StateCountyTractID",
  SUM(block_group_population) OVER (PARTITION BY "StateCountyTractID") AS "TotalTractPopulation",
  block_group_population / NULLIF(SUM(block_group_population) OVER (PARTITION BY "StateCountyTractID"), 0) AS "PopulationRatio"
FROM block_group_pop