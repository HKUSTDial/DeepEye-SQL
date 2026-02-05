SELECT
  za."zipcode_geom",
  za."area_land_meters",
  za."area_water_meters",
  za."latitude",
  za."longitude",
  za."state_code",
  za."state_name",
  za."city",
  za."county",
  pop."total_population"
FROM "CENSUS_BUREAU_USA"."UTILITY_US"."ZIPCODE_AREA" za
INNER JOIN (
  SELECT "zipcode", SUM("population") AS "total_population"
  FROM "CENSUS_BUREAU_USA"."CENSUS_BUREAU_USA"."POPULATION_BY_ZIP_2010"
  WHERE "gender" IN ('male', 'female')
    AND "minimum_age" IS NULL
    AND "maximum_age" IS NULL
  GROUP BY "zipcode"
) pop ON za."zipcode" = pop."zipcode"
WHERE ST_DWITHIN(TO_GEOGRAPHY(za."zipcode_geom"), ST_MAKEPOINT(-122.3321, 47.6062), 10000)