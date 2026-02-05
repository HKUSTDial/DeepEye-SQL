WITH metro_geo AS (
  SELECT "GEO_ID"
  FROM "US_REAL_ESTATE"."CYBERSYN"."GEOGRAPHY_INDEX"
  WHERE "GEO_NAME" LIKE '%Phoenix-Mesa-Scottsdale%'
  LIMIT 1
),
income_inflow AS (
  SELECT 
    MAX(CASE WHEN "DATE" = '2022-12-31' THEN "VALUE" END) AS income_2022,
    MAX(CASE WHEN "DATE" = '2023-12-31' THEN "VALUE" END) AS income_2023
  FROM "US_REAL_ESTATE"."CYBERSYN"."IRS_ORIGIN_DESTINATION_MIGRATION_TIMESERIES" mig
  INNER JOIN metro_geo mg ON mig."TO_GEO_ID" = mg."GEO_ID"
  WHERE mig."VARIABLE_NAME" = 'Adjusted Gross Income'
    AND mig."UNIT" = 'USD'
    AND mig."DATE" IN ('2022-12-31', '2023-12-31')
),
hpi_var AS (
  SELECT "VARIABLE"
  FROM "US_REAL_ESTATE"."CYBERSYN"."FHFA_HOUSE_PRICE_ATTRIBUTES"
  WHERE "SEASONALLY_ADJUSTED" = TRUE
    AND "INDEX_TYPE" = 'purchase-only'
    AND "FREQUENCY" = 'MONTHLY'
  LIMIT 1
),
hpi_timeseries AS (
  SELECT ts."DATE", ts."VALUE"
  FROM "US_REAL_ESTATE"."CYBERSYN"."FHFA_HOUSE_PRICE_TIMESERIES" ts
  INNER JOIN metro_geo mg ON ts."GEO_ID" = mg."GEO_ID"
  INNER JOIN hpi_var hv ON ts."VARIABLE" = hv."VARIABLE"
  WHERE ts."DATE" BETWEEN '2023-01-01' AND '2023-12-31'
),
hpi_min_max AS (
  SELECT MIN("DATE") AS min_date, MAX("DATE") AS max_date
  FROM hpi_timeseries
),
hpi_values AS (
  SELECT 
    (SELECT "VALUE" FROM hpi_timeseries WHERE "DATE" = (SELECT min_date FROM hpi_min_max)) AS start_value,
    (SELECT "VALUE" FROM hpi_timeseries WHERE "DATE" = (SELECT max_date FROM hpi_min_max)) AS end_value
)
SELECT 
  (income_2023 - income_2022) / NULLIF(income_2022, 0) * 100 AS income_inflow_pct_change,
  (end_value - start_value) / NULLIF(start_value, 0) * 100 AS home_price_index_pct_change
FROM income_inflow, hpi_values;