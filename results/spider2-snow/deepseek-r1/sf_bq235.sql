WITH outpatient_aggregates AS (
  SELECT 
    "provider_id",
    "provider_name",
    SUM("average_total_payments" * "outpatient_services") AS total_outpatient_payments,
    SUM("outpatient_services") AS total_outpatient_services
  FROM "CMS_DATA"."CMS_MEDICARE"."OUTPATIENT_CHARGES_2014"
  GROUP BY "provider_id", "provider_name"
),
inpatient_aggregates AS (
  SELECT 
    "provider_id",
    "provider_name",
    SUM("average_total_payments" * "total_discharges") AS total_inpatient_payments,
    SUM("total_discharges") AS total_inpatient_services
  FROM "CMS_DATA"."CMS_MEDICARE"."INPATIENT_CHARGES_2014"
  GROUP BY "provider_id", "provider_name"
),
combined AS (
  SELECT 
    o."provider_id",
    o."provider_name" AS "provider_name",
    (o.total_outpatient_payments + i.total_inpatient_payments) AS combined_total_payments,
    (o.total_outpatient_services + i.total_inpatient_services) AS combined_total_services,
    combined_total_payments / combined_total_services AS combined_average_cost
  FROM outpatient_aggregates o
  INNER JOIN inpatient_aggregates i ON o."provider_id" = i."provider_id"
)
SELECT 
  "provider_id",
  "provider_name",
  combined_average_cost
FROM combined
ORDER BY combined_average_cost DESC
LIMIT 1