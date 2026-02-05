WITH zip_totals AS (
    SELECT 
        f."ZipCode",
        SUM(f."CensusValueByZip") AS total_commuters
    FROM "CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE"."PUBLIC"."Fact_CensusValues_ACS2021_ByZip" f
    INNER JOIN "CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE"."PUBLIC"."LU_GeographyExpanded" g
        ON f."ZipCode" = g."ZipCode"
    WHERE g."PreferredStateAbbrev" = 'NY'
        AND f."MetricID" IN ('B08303_012E', 'B08303_013E')
    GROUP BY f."ZipCode"
),
state_totals AS (
    SELECT 
        SUM("StateBenchmarkValue") AS state_benchmark,
        MAX("TotalStatePopulation") AS state_population
    FROM "CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE"."PUBLIC"."Fact_StateBenchmark_ACS2021"
    WHERE "StateAbbrev" = 'NY'
        AND "MetricID" IN ('B08303_012E', 'B08303_013E')
)
SELECT 
    z."ZipCode",
    z.total_commuters,
    s.state_benchmark,
    s.state_population
FROM zip_totals z
CROSS JOIN state_totals s
ORDER BY z.total_commuters DESC
LIMIT 1