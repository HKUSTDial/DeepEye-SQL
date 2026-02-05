WITH providers_mv AS (
  SELECT
    "npi",
    "healthcare_provider_taxonomy_1_specialization" AS spec1,
    "healthcare_provider_taxonomy_2_specialization" AS spec2,
    "healthcare_provider_taxonomy_3_specialization" AS spec3,
    "healthcare_provider_taxonomy_4_specialization" AS spec4,
    "healthcare_provider_taxonomy_5_specialization" AS spec5,
    "healthcare_provider_taxonomy_6_specialization" AS spec6,
    "healthcare_provider_taxonomy_7_specialization" AS spec7,
    "healthcare_provider_taxonomy_8_specialization" AS spec8,
    "healthcare_provider_taxonomy_9_specialization" AS spec9,
    "healthcare_provider_taxonomy_10_specialization" AS spec10,
    "healthcare_provider_taxonomy_11_specialization" AS spec11,
    "healthcare_provider_taxonomy_12_specialization" AS spec12,
    "healthcare_provider_taxonomy_13_specialization" AS spec13,
    "healthcare_provider_taxonomy_14_specialization" AS spec14,
    "healthcare_provider_taxonomy_15_specialization" AS spec15
  FROM "NPPES"."NPPES"."NPI_OPTIMIZED"
  WHERE UPPER("provider_business_practice_location_address_city_name") = 'MOUNTAIN VIEW'
    AND "provider_business_practice_location_address_state_name" = 'CA'
),
all_specs AS (
  SELECT "npi", spec1 AS specialization FROM providers_mv WHERE spec1 IS NOT NULL AND spec1 <> ''
  UNION ALL
  SELECT "npi", spec2 AS specialization FROM providers_mv WHERE spec2 IS NOT NULL AND spec2 <> ''
  UNION ALL
  SELECT "npi", spec3 AS specialization FROM providers_mv WHERE spec3 IS NOT NULL AND spec3 <> ''
  UNION ALL
  SELECT "npi", spec4 AS specialization FROM providers_mv WHERE spec4 IS NOT NULL AND spec4 <> ''
  UNION ALL
  SELECT "npi", spec5 AS specialization FROM providers_mv WHERE spec5 IS NOT NULL AND spec5 <> ''
  UNION ALL
  SELECT "npi", spec6 AS specialization FROM providers_mv WHERE spec6 IS NOT NULL AND spec6 <> ''
  UNION ALL
  SELECT "npi", spec7 AS specialization FROM providers_mv WHERE spec7 IS NOT NULL AND spec7 <> ''
  UNION ALL
  SELECT "npi", spec8 AS specialization FROM providers_mv WHERE spec8 IS NOT NULL AND spec8 <> ''
  UNION ALL
  SELECT "npi", spec9 AS specialization FROM providers_mv WHERE spec9 IS NOT NULL AND spec9 <> ''
  UNION ALL
  SELECT "npi", spec10 AS specialization FROM providers_mv WHERE spec10 IS NOT NULL AND spec10 <> ''
  UNION ALL
  SELECT "npi", spec11 AS specialization FROM providers_mv WHERE spec11 IS NOT NULL AND spec11 <> ''
  UNION ALL
  SELECT "npi", spec12 AS specialization FROM providers_mv WHERE spec12 IS NOT NULL AND spec12 <> ''
  UNION ALL
  SELECT "npi", spec13 AS specialization FROM providers_mv WHERE spec13 IS NOT NULL AND spec13 <> ''
  UNION ALL
  SELECT "npi", spec14 AS specialization FROM providers_mv WHERE spec14 IS NOT NULL AND spec14 <> ''
  UNION ALL
  SELECT "npi", spec15 AS specialization FROM providers_mv WHERE spec15 IS NOT NULL AND spec15 <> ''
),
spec_counts AS (
  SELECT specialization, COUNT(DISTINCT "npi") AS npi_count
  FROM all_specs
  GROUP BY specialization
),
top10 AS (
  SELECT specialization, npi_count
  FROM spec_counts
  ORDER BY npi_count DESC
  LIMIT 10
),
avg_count AS (
  SELECT AVG(npi_count) AS avg_npi_count
  FROM top10
)
SELECT t.specialization, t.npi_count
FROM top10 t
CROSS JOIN avg_count a
ORDER BY ABS(t.npi_count - a.avg_npi_count)
LIMIT 1