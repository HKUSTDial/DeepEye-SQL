WITH over_one_hour_metrics AS (
  SELECT "MetricID"
  FROM "CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE"."PUBLIC"."Dim_CensusMetrics"
  WHERE "MetricTableID" = 'B08303'
    AND ("Descriptor1" LIKE '%60%' OR "Descriptor1" LIKE '%90%' OR "Descriptor1" LIKE '%over%')
),
zip_commuters AS (
  SELECT l."ZipCode", SUM(f."CensusValueByZip") AS total_commuters
  FROM "CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE"."PUBLIC"."LU_GeographyExpanded" l
  JOIN "CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE"."PUBLIC"."Fact_CensusValues_ACS2021_ByZip" f
    ON l."ZipCode" = f."ZipCode"
  WHERE l."PreferredStateAbbrev" = 'NY'
    AND f."MetricID" IN (SELECT "MetricID" FROM over_one_hour_metrics)
  GROUP BY l."ZipCode"
),
state_data AS (
  SELECT SUM("StateBenchmarkValue") AS state_benchmark,
         MAX("TotalStatePopulation") AS state_population
  FROM "CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE"."PUBLIC"."Fact_StateBenchmark_ACS2021"
  WHERE "StateAbbrev" = 'NY'
    AND "MetricID" IN (SELECT "MetricID" FROM over_one_hour_metrics)
),
top_zip AS (
  SELECT "ZipCode", total_commuters
  FROM zip_commuters
  ORDER BY total_commuters DESC
  LIMIT 1
)
SELECT t."ZipCode",
       t.total_commuters AS "Total Commuters",
       s.state_benchmark AS "State Benchmark",
       s.state_population AS "State Population"
FROM top_zip t
CROSS JOIN state_data s