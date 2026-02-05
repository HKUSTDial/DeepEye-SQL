WITH filtered_providers AS (
  SELECT 
    npi,
    healthcare_provider_taxonomy_code_1,
    healthcare_provider_taxonomy_code_2,
    healthcare_provider_taxonomy_code_3,
    healthcare_provider_taxonomy_code_4,
    healthcare_provider_taxonomy_code_5,
    healthcare_provider_taxonomy_code_6,
    healthcare_provider_taxonomy_code_7,
    healthcare_provider_taxonomy_code_8,
    healthcare_provider_taxonomy_code_9,
    healthcare_provider_taxonomy_code_10,
    healthcare_provider_taxonomy_code_11,
    healthcare_provider_taxonomy_code_12,
    healthcare_provider_taxonomy_code_13,
    healthcare_provider_taxonomy_code_14,
    healthcare_provider_taxonomy_code_15
  FROM `bigquery-public-data.nppes.npi_optimized`
  WHERE UPPER(TRIM(provider_business_practice_location_address_city_name)) = 'MOUNTAIN VIEW'
    AND UPPER(TRIM(provider_business_practice_location_address_state_name)) IN ('CA', 'CALIFORNIA')
),
unpivoted_codes AS (
  SELECT DISTINCT
    npi,
    TRIM(code) AS code
  FROM filtered_providers
  CROSS JOIN UNNEST([
    healthcare_provider_taxonomy_code_1,
    healthcare_provider_taxonomy_code_2,
    healthcare_provider_taxonomy_code_3,
    healthcare_provider_taxonomy_code_4,
    healthcare_provider_taxonomy_code_5,
    healthcare_provider_taxonomy_code_6,
    healthcare_provider_taxonomy_code_7,
    healthcare_provider_taxonomy_code_8,
    healthcare_provider_taxonomy_code_9,
    healthcare_provider_taxonomy_code_10,
    healthcare_provider_taxonomy_code_11,
    healthcare_provider_taxonomy_code_12,
    healthcare_provider_taxonomy_code_13,
    healthcare_provider_taxonomy_code_14,
    healthcare_provider_taxonomy_code_15
  ]) AS code
  WHERE code IS NOT NULL AND code != ''
),
taxonomy_mapping AS (
  SELECT code, specialization
  FROM (
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_130` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_120` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_151` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_160` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_150` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_101` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_091` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_111` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_161` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_110` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_140` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_100` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_090` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_131` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_170` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_121` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set_141` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
    UNION ALL
    SELECT TRIM(code) AS code, TRIM(specialization) AS specialization FROM `bigquery-public-data.nppes.healthcare_provider_taxonomy_code_set` WHERE TRIM(code) != '' AND TRIM(specialization) != ''
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY specialization) = 1
),
provider_specializations AS (
  SELECT DISTINCT
    u.npi,
    t.specialization
  FROM unpivoted_codes u
  INNER JOIN taxonomy_mapping t ON u.code = t.code
),
specialization_counts AS (
  SELECT
    specialization,
    COUNT(DISTINCT npi) AS npi_count
  FROM provider_specializations
  GROUP BY specialization
),
top_10 AS (
  SELECT
    specialization,
    npi_count
  FROM specialization_counts
  ORDER BY npi_count DESC
  LIMIT 10
),
avg_count AS (
  SELECT AVG(npi_count) AS avg_npi_count FROM top_10
)
SELECT
  specialization,
  npi_count
FROM top_10
CROSS JOIN avg_count
ORDER BY ABS(npi_count - avg_npi_count), specialization
LIMIT 1