WITH active_banks AS (
  SELECT CAST(e."ID_RSSD" AS TEXT) AS "ID_RSSD", e."NAME"
  FROM "FINANCE__ECONOMICS"."CYBERSYN"."FINANCIAL_INSTITUTION_ENTITIES" e
  WHERE e."IS_ACTIVE" = TRUE
),
bank_data AS (
  SELECT 
    t."ID_RSSD",
    MAX(CASE WHEN a."VARIABLE_NAME" LIKE '%Assets%' AND a."UNIT" = 'USD' THEN t."VALUE" END) AS assets_value,
    MAX(CASE WHEN a."VARIABLE_NAME" LIKE '%Insured (Estimated)%' THEN t."VALUE" END) AS insured_pct_value
  FROM "FINANCE__ECONOMICS"."CYBERSYN"."FINANCIAL_INSTITUTION_TIMESERIES" t
  INNER JOIN "FINANCE__ECONOMICS"."CYBERSYN"."FINANCIAL_INSTITUTION_ATTRIBUTES" a
    ON t."VARIABLE" = a."VARIABLE" 
    AND t."UNIT" = a."UNIT"
    AND a."FREQUENCY" = 'Quarterly'
  WHERE t."DATE" = '2022-12-31'
    AND (a."VARIABLE_NAME" LIKE '%Assets%' OR a."VARIABLE_NAME" LIKE '%Insured (Estimated)%')
  GROUP BY t."ID_RSSD"
  HAVING assets_value > 10000000000
    AND insured_pct_value IS NOT NULL
)
SELECT 
  ab."NAME" AS bank_name,
  (1 - bd.insured_pct_value) * 100 AS uninsured_assets_percentage
FROM active_banks ab
INNER JOIN bank_data bd ON ab."ID_RSSD" = bd."ID_RSSD"
ORDER BY uninsured_assets_percentage DESC
LIMIT 10