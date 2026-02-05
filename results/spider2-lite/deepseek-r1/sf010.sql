WITH california AS (
  SELECT "GEO_ID"
  FROM "US_REAL_ESTATE"."CYBERSYN"."GEOGRAPHY_INDEX"
  WHERE "LEVEL" = 'State' AND "GEO_NAME" = 'California'
),
relevant_vars AS (
  SELECT "VARIABLE",
    CASE
      WHEN "VARIABLE_GROUP" LIKE '%90 to 180%' OR "VARIABLE_NAME" LIKE '%90 to 180%' THEN 'past_due_90_180'
      WHEN "VARIABLE_GROUP" LIKE '%Forbearance%' OR "VARIABLE_NAME" LIKE '%Forbearance%' THEN 'forbearance'
      WHEN "VARIABLE_GROUP" LIKE '%Foreclosure%' OR "VARIABLE_NAME" LIKE '%Foreclosure%' THEN 'foreclosure'
      WHEN "VARIABLE_GROUP" LIKE '%Bankruptcy%' OR "VARIABLE_NAME" LIKE '%Bankruptcy%' THEN 'bankruptcy'
      WHEN "VARIABLE_GROUP" LIKE '%Deed-in-Lieu%' OR "VARIABLE_NAME" LIKE '%Deed-in-Lieu%' THEN 'deed_in_lieu'
      ELSE 'other'
    END AS category
  FROM "US_REAL_ESTATE"."CYBERSYN"."FHFA_MORTGAGE_PERFORMANCE_ATTRIBUTES"
  WHERE "UNIT" = 'Percent'
    AND (
      "VARIABLE_GROUP" LIKE '%90 to 180%' OR "VARIABLE_NAME" LIKE '%90 to 180%'
      OR "VARIABLE_GROUP" LIKE '%Forbearance%' OR "VARIABLE_NAME" LIKE '%Forbearance%'
      OR "VARIABLE_GROUP" LIKE '%Foreclosure%' OR "VARIABLE_NAME" LIKE '%Foreclosure%'
      OR "VARIABLE_GROUP" LIKE '%Bankruptcy%' OR "VARIABLE_NAME" LIKE '%Bankruptcy%'
      OR "VARIABLE_GROUP" LIKE '%Deed-in-Lieu%' OR "VARIABLE_NAME" LIKE '%Deed-in-Lieu%'
    )
)
SELECT
  t."DATE",
  SUM(CASE WHEN a.category = 'past_due_90_180' THEN t."VALUE" ELSE 0 END) AS "90_180_days_past_due",
  SUM(CASE WHEN a.category = 'forbearance' THEN t."VALUE" ELSE 0 END) AS "forbearance",
  SUM(CASE WHEN a.category = 'foreclosure' THEN t."VALUE" ELSE 0 END) AS "foreclosure",
  SUM(CASE WHEN a.category = 'bankruptcy' THEN t."VALUE" ELSE 0 END) AS "bankruptcy",
  SUM(CASE WHEN a.category = 'deed_in_lieu' THEN t."VALUE" ELSE 0 END) AS "deed_in_lieu",
  SUM(t."VALUE") AS "cumulative_ratio"
FROM "US_REAL_ESTATE"."CYBERSYN"."FHFA_MORTGAGE_PERFORMANCE_TIMESERIES" t
JOIN relevant_vars a ON t."VARIABLE" = a."VARIABLE"
JOIN california c ON t."GEO_ID" = c."GEO_ID"
WHERE t."DATE" >= '2023-01-01' AND t."DATE" <= '2023-12-31'
GROUP BY t."DATE"
ORDER BY t."DATE"