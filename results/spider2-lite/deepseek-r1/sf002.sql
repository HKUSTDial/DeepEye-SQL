WITH active_banks AS (
    SELECT CAST("ID_RSSD" AS VARCHAR) AS id_rssd_text, "NAME"
    FROM "FINANCE__ECONOMICS"."CYBERSYN"."FINANCIAL_INSTITUTION_ENTITIES"
    WHERE "IS_ACTIVE" = TRUE AND "CATEGORY" = 'Bank'
),
timeseries_data AS (
    SELECT 
        t."ID_RSSD",
        t."VARIABLE_NAME",
        t."VALUE",
        t."UNIT"
    FROM "FINANCE__ECONOMICS"."CYBERSYN"."FINANCIAL_INSTITUTION_TIMESERIES" t
    INNER JOIN active_banks b ON t."ID_RSSD" = b.id_rssd_text
    WHERE t."DATE" = '2022-12-31'
      AND (t."VARIABLE_NAME" = 'Total Assets' OR t."VARIABLE_NAME" LIKE '%Insured (Estimated)%')
),
pivoted AS (
    SELECT 
        "ID_RSSD",
        MAX(CASE WHEN "VARIABLE_NAME" = 'Total Assets' THEN "VALUE" END) AS assets_value,
        MAX(CASE WHEN "VARIABLE_NAME" LIKE '%Insured (Estimated)%' THEN "VALUE" END) AS insured_value,
        MAX(CASE WHEN "VARIABLE_NAME" LIKE '%Insured (Estimated)%' THEN "UNIT" END) AS insured_unit
    FROM timeseries_data
    GROUP BY "ID_RSSD"
    HAVING assets_value > 10000000000 AND insured_value IS NOT NULL
)
SELECT 
    b."NAME",
    CASE 
        WHEN p.insured_unit = 'Percent' THEN 100 - p.insured_value
        ELSE (1 - p.insured_value) * 100
    END AS uninsured_percentage
FROM pivoted p
JOIN active_banks b ON p."ID_RSSD" = b.id_rssd_text
ORDER BY uninsured_percentage DESC
LIMIT 10