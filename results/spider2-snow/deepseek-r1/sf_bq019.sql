WITH drg_total AS (
  SELECT "drg_definition", SUM("total_discharges") AS total_drg_discharges
  FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2014"
  GROUP BY "drg_definition"
),
top_drg AS (
  SELECT "drg_definition", total_drg_discharges
  FROM drg_total
  ORDER BY total_drg_discharges DESC
  LIMIT 1
),
city_discharges AS (
  SELECT "provider_city", SUM("total_discharges") AS city_total_discharges
  FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2014"
  WHERE "drg_definition" = (SELECT "drg_definition" FROM top_drg)
  GROUP BY "provider_city"
  ORDER BY city_total_discharges DESC
  LIMIT 3
),
weighted_avg AS (
  SELECT 
    t."provider_city",
    SUM(t."total_discharges" * t."average_total_payments") / SUM(t."total_discharges") AS weighted_avg_payments
  FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2014" t
  WHERE t."drg_definition" = (SELECT "drg_definition" FROM top_drg)
    AND t."provider_city" IN (SELECT "provider_city" FROM city_discharges)
  GROUP BY t."provider_city"
)
SELECT 
  (SELECT "drg_definition" FROM top_drg) AS "drg_definition",
  w."provider_city",
  w.weighted_avg_payments
FROM weighted_avg w
ORDER BY w."provider_city";