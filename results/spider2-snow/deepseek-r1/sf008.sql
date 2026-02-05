WITH metro AS (
  SELECT "GEO_ID", "GEO_NAME"
  FROM "US_REAL_ESTATE"."CYBERSYN"."GEOGRAPHY_INDEX"
  WHERE "GEO_NAME" LIKE '%Phoenix-Mesa-Scottsdale%'
), counties AS (
  SELECT gi."GEO_ID" AS county_geo_id
  FROM "US_REAL_ESTATE"."CYBERSYN"."GEOGRAPHY_RELATIONSHIPS" r
  JOIN metro m ON r."GEO_ID" = m."GEO_ID"
  JOIN "US_REAL_ESTATE"."CYBERSYN"."GEOGRAPHY_INDEX" gi ON r."RELATED_GEO_ID" = gi."GEO_ID"
  WHERE r."RELATIONSHIP_TYPE" = 'Contains'
    AND gi."LEVEL" = 'County'
), inflow_sums AS (
  SELECT 
    "DATE",
    SUM("VALUE") AS total_inflow
  FROM "US_REAL_ESTATE"."CYBERSYN"."IRS_ORIGIN_DESTINATION_MIGRATION_TIMESERIES"
  WHERE "TO_GEO_ID" IN (SELECT county_geo_id FROM counties)
    AND "VARIABLE_NAME" = 'Adjusted Gross Income'
    AND "UNIT" = 'USD'
    AND "DATE" IN ('2022-12-31', '2023-12-31')
  GROUP BY "DATE"
), inflow_pivot AS (
  SELECT
    MAX(CASE WHEN "DATE" = '2022-12-31' THEN total_inflow END) AS inflow_2022,
    MAX(CASE WHEN "DATE" = '2023-12-31' THEN total_inflow END) AS inflow_2023
  FROM inflow_sums
), inflow_pct_change AS (
  SELECT 
    (inflow_2023 - inflow_2022) / NULLIF(inflow_2022, 0) * 100 AS gross_income_inflow_pct_change
  FROM inflow_pivot
), hpi_variable AS (
  SELECT "VARIABLE"
  FROM "US_REAL_ESTATE"."CYBERSYN"."FHFA_HOUSE_PRICE_ATTRIBUTES"
  WHERE "INDEX_TYPE" = 'purchase-only'
    AND "SEASONALLY_ADJUSTED" = TRUE
    AND "FREQUENCY" = 'QUARTERLY'
  LIMIT 1
), hpi_values AS (
  SELECT 
    "DATE",
    "VALUE"
  FROM "US_REAL_ESTATE"."CYBERSYN"."FHFA_HOUSE_PRICE_TIMESERIES" ts
  JOIN hpi_variable var ON ts."VARIABLE" = var."VARIABLE"
  JOIN metro m ON ts."GEO_ID" = m."GEO_ID"
  WHERE "DATE" IN ('2022-12-31', '2023-12-31')
), hpi_pivot AS (
  SELECT
    MAX(CASE WHEN "DATE" = '2022-12-31' THEN "VALUE" END) AS hpi_2022,
    MAX(CASE WHEN "DATE" = '2023-12-31' THEN "VALUE" END) AS hpi_2023
  FROM hpi_values
), hpi_pct_change AS (
  SELECT 
    (hpi_2023 - hpi_2022) / NULLIF(hpi_2022, 0) * 100 AS home_price_index_pct_change
  FROM hpi_pivot
)
SELECT 
  gross_income_inflow_pct_change,
  home_price_index_pct_change
FROM inflow_pct_change
CROSS JOIN hpi_pct_change