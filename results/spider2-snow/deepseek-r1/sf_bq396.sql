SELECT
  "state_name",
  ABS(
    COUNT(CASE WHEN "atmospheric_conditions_name" ILIKE '%rain%' THEN 1 END) -
    COUNT(CASE WHEN "atmospheric_conditions_name" ILIKE '%clear%' THEN 1 END)
  ) AS "difference"
FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."ACCIDENT_2016"
WHERE "year_of_crash" = 2016
  AND "day_of_week" IN (1, 7)
GROUP BY "state_name"
HAVING 
  COUNT(CASE WHEN "atmospheric_conditions_name" ILIKE '%rain%' THEN 1 END) > 0
  OR COUNT(CASE WHEN "atmospheric_conditions_name" ILIKE '%clear%' THEN 1 END) > 0
ORDER BY "difference" DESC
LIMIT 3