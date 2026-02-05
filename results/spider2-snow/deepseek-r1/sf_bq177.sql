WITH inpatient_all AS (
    SELECT "provider_id", "average_medicare_payments", "total_discharges", 2011 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2011"
    UNION ALL
    SELECT "provider_id", "average_medicare_payments", "total_discharges", 2012 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2012"
    UNION ALL
    SELECT "provider_id", "average_medicare_payments", "total_discharges", 2013 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2013"
    UNION ALL
    SELECT "provider_id", "average_medicare_payments", "total_discharges", 2014 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2014"
    UNION ALL
    SELECT "provider_id", "average_medicare_payments", "total_discharges", 2015 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2015"
), outpatient_all AS (
    SELECT "provider_id", "average_total_payments", "outpatient_services", 2011 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."OUTPATIENT_CHARGES_2011"
    UNION ALL
    SELECT "provider_id", "average_total_payments", "outpatient_services", 2012 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."OUTPATIENT_CHARGES_2012"
    UNION ALL
    SELECT "provider_id", "average_total_payments", "outpatient_services", 2013 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."OUTPATIENT_CHARGES_2013"
    UNION ALL
    SELECT "provider_id", "average_total_payments", "outpatient_services", 2014 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."OUTPATIENT_CHARGES_2014"
    UNION ALL
    SELECT "provider_id", "average_total_payments", "outpatient_services", 2015 AS "year" FROM "CMS_DATA"."CMS_MEDICARE"."OUTPATIENT_CHARGES_2015"
), provider_total AS (
    SELECT "provider_id", SUM("average_medicare_payments" * "total_discharges") AS "total_inpatient_cost" FROM inpatient_all GROUP BY "provider_id" ORDER BY "total_inpatient_cost" DESC LIMIT 1
), inpatient_avg_per_year AS (
    SELECT "year", AVG("average_medicare_payments" * "total_discharges") AS "avg_inpatient_cost" FROM inpatient_all WHERE "provider_id" = (SELECT "provider_id" FROM provider_total) GROUP BY "year"
), outpatient_avg_per_year AS (
    SELECT "year", AVG("average_total_payments" * "outpatient_services") AS "avg_outpatient_cost" FROM outpatient_all WHERE "provider_id" = (SELECT "provider_id" FROM provider_total) GROUP BY "year"
)
SELECT COALESCE(i."year", o."year") AS "year", i."avg_inpatient_cost", o."avg_outpatient_cost" FROM inpatient_avg_per_year i FULL OUTER JOIN outpatient_avg_per_year o ON i."year" = o."year" ORDER BY "year"