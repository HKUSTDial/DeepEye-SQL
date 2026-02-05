WITH "utah_counties" AS (
    SELECT "county_fips_code", "county_name"
    FROM "BLS"."GEO_US_BOUNDARIES"."COUNTIES"
    WHERE "state_fips_code" = '49'
),
"year_2000" AS (
    SELECT "area_fips", AVG("month3_emplvl_23_construction") AS "avg_2000"
    FROM (
        SELECT "area_fips", "month3_emplvl_23_construction" FROM "BLS"."BLS_QCEW"."_2000_Q1"
        UNION ALL
        SELECT "area_fips", "month3_emplvl_23_construction" FROM "BLS"."BLS_QCEW"."_2000_Q2"
        UNION ALL
        SELECT "area_fips", "month3_emplvl_23_construction" FROM "BLS"."BLS_QCEW"."_2000_Q3"
        UNION ALL
        SELECT "area_fips", "month3_emplvl_23_construction" FROM "BLS"."BLS_QCEW"."_2000_Q4"
    ) t
    GROUP BY "area_fips"
),
"year_2018" AS (
    SELECT "area_fips", AVG("month3_emplvl_23_construction") AS "avg_2018"
    FROM (
        SELECT "area_fips", "month3_emplvl_23_construction" FROM "BLS"."BLS_QCEW"."_2018_Q1"
        UNION ALL
        SELECT "area_fips", "month3_emplvl_23_construction" FROM "BLS"."BLS_QCEW"."_2018_Q2"
        UNION ALL
        SELECT "area_fips", "month3_emplvl_23_construction" FROM "BLS"."BLS_QCEW"."_2018_Q3"
        UNION ALL
        SELECT "area_fips", "month3_emplvl_23_construction" FROM "BLS"."BLS_QCEW"."_2018_Q4"
    ) t
    GROUP BY "area_fips"
)
SELECT 
    "utah_counties"."county_name",
    (("year_2018"."avg_2018" - "year_2000"."avg_2000") / "year_2000"."avg_2000") * 100 AS "pct_increase"
FROM "utah_counties"
INNER JOIN "year_2000" ON "utah_counties"."county_fips_code" = "year_2000"."area_fips"
INNER JOIN "year_2018" ON "utah_counties"."county_fips_code" = "year_2018"."area_fips"
WHERE "year_2000"."avg_2000" > 0
ORDER BY "pct_increase" DESC
LIMIT 1