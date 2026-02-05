WITH kings_tracts AS (
    SELECT "geo_id"
    FROM "FEC"."GEO_CENSUS_TRACTS"."US_CENSUS_TRACTS_NATIONAL"
    WHERE "state_fips_code" = '36' AND "county_fips_code" = '047'
), median_income_data AS (
    SELECT "geo_id", "median_income"
    FROM "FEC"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2018_5YR"
    WHERE "geo_id" IN (SELECT "geo_id" FROM kings_tracts)
), ny_donations AS (
    SELECT LEFT("zip_code", 5) AS "zip5", "transaction_amt"
    FROM "FEC"."FEC"."INDIVIDUALS_INGEST_2020"
    WHERE "state" = 'NY'
), dominant_tract_mapping AS (
    SELECT "zip_code", "census_tract_geoid", ROW_NUMBER() OVER (PARTITION BY "zip_code" ORDER BY "residential_ratio" DESC) AS "rn"
    FROM "FEC"."HUD_ZIPCODE_CROSSWALK"."ZIPCODE_TO_CENSUS_TRACTS"
), zip_to_tract AS (
    SELECT "zip_code", "census_tract_geoid"
    FROM dominant_tract_mapping
    WHERE "rn" = 1
), donations_by_tract AS (
    SELECT z."census_tract_geoid", AVG(n."transaction_amt") AS "avg_donation"
    FROM ny_donations n
    INNER JOIN zip_to_tract z ON n."zip5" = z."zip_code"
    GROUP BY z."census_tract_geoid"
)
SELECT k."geo_id" AS "census_tract", d."avg_donation" AS "average_political_donation", m."median_income"
FROM kings_tracts k
LEFT JOIN median_income_data m ON k."geo_id" = m."geo_id"
LEFT JOIN donations_by_tract d ON k."geo_id" = d."census_tract_geoid"
ORDER BY k."geo_id"