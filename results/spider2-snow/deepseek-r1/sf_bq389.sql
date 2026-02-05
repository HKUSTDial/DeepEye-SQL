WITH months AS (
  SELECT 1 AS "month" UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
), pm10 AS (
  SELECT EXTRACT(MONTH FROM "date_local") AS "month", AVG("arithmetic_mean") AS "avg_pm10"
  FROM "EPA_HISTORICAL_AIR_QUALITY"."EPA_HISTORICAL_AIR_QUALITY"."PM10_DAILY_SUMMARY"
  WHERE "state_name" = 'California' AND EXTRACT(YEAR FROM "date_local") = 2020
  GROUP BY "month"
), pm25_frm AS (
  SELECT EXTRACT(MONTH FROM "date_local") AS "month", AVG("arithmetic_mean") AS "avg_pm25_frm"
  FROM "EPA_HISTORICAL_AIR_QUALITY"."EPA_HISTORICAL_AIR_QUALITY"."PM25_FRM_DAILY_SUMMARY"
  WHERE "state_name" = 'California' AND EXTRACT(YEAR FROM "date_local") = 2020
  GROUP BY "month"
), pm25_nonfrm AS (
  SELECT EXTRACT(MONTH FROM "date_local") AS "month", AVG("arithmetic_mean") AS "avg_pm25_nonfrm"
  FROM "EPA_HISTORICAL_AIR_QUALITY"."EPA_HISTORICAL_AIR_QUALITY"."PM25_NONFRM_DAILY_SUMMARY"
  WHERE "state_name" = 'California' AND EXTRACT(YEAR FROM "date_local") = 2020
  GROUP BY "month"
), voc AS (
  SELECT EXTRACT(MONTH FROM "date_local") AS "month", AVG("arithmetic_mean") AS "avg_voc"
  FROM "EPA_HISTORICAL_AIR_QUALITY"."EPA_HISTORICAL_AIR_QUALITY"."VOC_DAILY_SUMMARY"
  WHERE "state_name" = 'California' AND EXTRACT(YEAR FROM "date_local") = 2020
  GROUP BY "month"
), so2 AS (
  SELECT EXTRACT(MONTH FROM "date_local") AS "month", AVG("arithmetic_mean") * 10 AS "avg_so2_scaled"
  FROM "EPA_HISTORICAL_AIR_QUALITY"."EPA_HISTORICAL_AIR_QUALITY"."SO2_DAILY_SUMMARY"
  WHERE "state_name" = 'California' AND EXTRACT(YEAR FROM "date_local") = 2020
  GROUP BY "month"
), lead AS (
  SELECT EXTRACT(MONTH FROM "date_local") AS "month", AVG("arithmetic_mean") * 100 AS "avg_lead_scaled"
  FROM "EPA_HISTORICAL_AIR_QUALITY"."EPA_HISTORICAL_AIR_QUALITY"."LEAD_DAILY_SUMMARY"
  WHERE "state_name" = 'California' AND EXTRACT(YEAR FROM "date_local") = 2020
  GROUP BY "month"
)
SELECT m."month", pm10."avg_pm10", pm25_frm."avg_pm25_frm", pm25_nonfrm."avg_pm25_nonfrm", voc."avg_voc", so2."avg_so2_scaled", lead."avg_lead_scaled"
FROM months m
LEFT JOIN pm10 ON m."month" = pm10."month"
LEFT JOIN pm25_frm ON m."month" = pm25_frm."month"
LEFT JOIN pm25_nonfrm ON m."month" = pm25_nonfrm."month"
LEFT JOIN voc ON m."month" = voc."month"
LEFT JOIN so2 ON m."month" = so2."month"
LEFT JOIN lead ON m."month" = lead."month"
ORDER BY m."month"