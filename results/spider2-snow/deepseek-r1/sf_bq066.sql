WITH natality AS (
    SELECT 
        "County_of_Residence_FIPS" AS fips,
        EXTRACT(YEAR FROM "Year") AS birth_year,
        SUM(CASE WHEN "Maternal_Morbidity_Desc" = 'None checked' THEN "Births" ELSE 0 END) AS no_morbidity_births,
        SUM("Births") AS total_births
    FROM "SDOH"."SDOH_CDC_WONDER_NATALITY"."COUNTY_NATALITY_BY_MATERNAL_MORBIDITY"
    WHERE EXTRACT(YEAR FROM "Year") BETWEEN 2016 AND 2018
    GROUP BY "County_of_Residence_FIPS", EXTRACT(YEAR FROM "Year")
    HAVING SUM("Births") > 0
),
census AS (
    SELECT '2015' AS census_year, "geo_id", "poverty" FROM "SDOH"."CENSUS_BUREAU_ACS"."COUNTY_2015_5YR"
    UNION ALL
    SELECT '2016' AS census_year, "geo_id", "poverty" FROM "SDOH"."CENSUS_BUREAU_ACS"."COUNTY_2016_5YR"
    UNION ALL
    SELECT '2017' AS census_year, "geo_id", "poverty" FROM "SDOH"."CENSUS_BUREAU_ACS"."COUNTY_2017_5YR"
)
SELECT 
    CORR(c."poverty", n.no_morbidity_births * 100.0 / n.total_births) AS pearson_correlation
FROM natality n
INNER JOIN census c 
    ON n.fips = c."geo_id" 
    AND n.birth_year = CAST(c.census_year AS INTEGER) + 1