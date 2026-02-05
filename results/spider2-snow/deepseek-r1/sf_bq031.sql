WITH station_data AS (
  SELECT 
    TO_DATE(g."year" || '-' || LPAD(g."mo", 2, '0') || '-' || LPAD(g."da", 2, '0'), 'YYYY-MM-DD') AS "date",
    (g."temp" - 32) * 5.0 / 9.0 AS temp_c,
    g."prcp" * 2.54 AS prcp_cm,
    CAST(g."wdsp" AS FLOAT) * 0.514444 AS wdsp_mps
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2019" AS g
  INNER JOIN "NOAA_DATA"."NOAA_GSOD"."STATIONS" AS s
    ON (g."wban" = s."wban" OR g."stn" = s."usaf")
  WHERE 
    UPPER(s."name") LIKE '%ROCHESTER%'
    AND g."temp" != 9999.9
    AND g."prcp" != 99.99
    AND g."wdsp" != '999.9'
    AND g."year" = '2019'
    AND TO_DATE(g."year" || '-' || LPAD(g."mo", 2, '0') || '-' || LPAD(g."da", 2, '0'), 'YYYY-MM-DD') BETWEEN '2019-01-01' AND '2019-03-31'
),
rochester_data AS (
  SELECT 
    "date",
    AVG(temp_c) AS temp_c,
    AVG(prcp_cm) AS prcp_cm,
    AVG(wdsp_mps) AS wdsp_mps
  FROM station_data
  GROUP BY "date"
),
moving_avgs AS (
  SELECT 
    "date",
    temp_c,
    prcp_cm,
    wdsp_mps,
    AVG(temp_c) OVER (ORDER BY "date" ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_temp_8d,
    AVG(prcp_cm) OVER (ORDER BY "date" ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_prcp_8d,
    AVG(wdsp_mps) OVER (ORDER BY "date" ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_wdsp_8d
  FROM rochester_data
),
lag_diffs AS (
  SELECT 
    "date",
    temp_c,
    prcp_cm,
    wdsp_mps,
    avg_temp_8d,
    avg_prcp_8d,
    avg_wdsp_8d,
    avg_temp_8d - LAG(avg_temp_8d, 1) OVER (ORDER BY "date") AS diff_temp_lag1,
    avg_temp_8d - LAG(avg_temp_8d, 2) OVER (ORDER BY "date") AS diff_temp_lag2,
    avg_temp_8d - LAG(avg_temp_8d, 3) OVER (ORDER BY "date") AS diff_temp_lag3,
    avg_temp_8d - LAG(avg_temp_8d, 4) OVER (ORDER BY "date") AS diff_temp_lag4,
    avg_temp_8d - LAG(avg_temp_8d, 5) OVER (ORDER BY "date") AS diff_temp_lag5,
    avg_temp_8d - LAG(avg_temp_8d, 6) OVER (ORDER BY "date") AS diff_temp_lag6,
    avg_temp_8d - LAG(avg_temp_8d, 7) OVER (ORDER BY "date") AS diff_temp_lag7,
    avg_temp_8d - LAG(avg_temp_8d, 8) OVER (ORDER BY "date") AS diff_temp_lag8,
    avg_prcp_8d - LAG(avg_prcp_8d, 1) OVER (ORDER BY "date") AS diff_prcp_lag1,
    avg_prcp_8d - LAG(avg_prcp_8d, 2) OVER (ORDER BY "date") AS diff_prcp_lag2,
    avg_prcp_8d - LAG(avg_prcp_8d, 3) OVER (ORDER BY "date") AS diff_prcp_lag3,
    avg_prcp_8d - LAG(avg_prcp_8d, 4) OVER (ORDER BY "date") AS diff_prcp_lag4,
    avg_prcp_8d - LAG(avg_prcp_8d, 5) OVER (ORDER BY "date") AS diff_prcp_lag5,
    avg_prcp_8d - LAG(avg_prcp_8d, 6) OVER (ORDER BY "date") AS diff_prcp_lag6,
    avg_prcp_8d - LAG(avg_prcp_8d, 7) OVER (ORDER BY "date") AS diff_prcp_lag7,
    avg_prcp_8d - LAG(avg_prcp_8d, 8) OVER (ORDER BY "date") AS diff_prcp_lag8,
    avg_wdsp_8d - LAG(avg_wdsp_8d, 1) OVER (ORDER BY "date") AS diff_wdsp_lag1,
    avg_wdsp_8d - LAG(avg_wdsp_8d, 2) OVER (ORDER BY "date") AS diff_wdsp_lag2,
    avg_wdsp_8d - LAG(avg_wdsp_8d, 3) OVER (ORDER BY "date") AS diff_wdsp_lag3,
    avg_wdsp_8d - LAG(avg_wdsp_8d, 4) OVER (ORDER BY "date") AS diff_wdsp_lag4,
    avg_wdsp_8d - LAG(avg_wdsp_8d, 5) OVER (ORDER BY "date") AS diff_wdsp_lag5,
    avg_wdsp_8d - LAG(avg_wdsp_8d, 6) OVER (ORDER BY "date") AS diff_wdsp_lag6,
    avg_wdsp_8d - LAG(avg_wdsp_8d, 7) OVER (ORDER BY "date") AS diff_wdsp_lag7,
    avg_wdsp_8d - LAG(avg_wdsp_8d, 8) OVER (ORDER BY "date") AS diff_wdsp_lag8
  FROM moving_avgs
)
SELECT 
  "date",
  ROUND(temp_c, 1) AS temp_c,
  ROUND(prcp_cm, 1) AS prcp_cm,
  ROUND(wdsp_mps, 1) AS wdsp_mps,
  ROUND(avg_temp_8d, 1) AS avg_temp_8d,
  ROUND(avg_prcp_8d, 1) AS avg_prcp_8d,
  ROUND(avg_wdsp_8d, 1) AS avg_wdsp_8d,
  ROUND(diff_temp_lag1, 1) AS diff_temp_lag1,
  ROUND(diff_temp_lag2, 1) AS diff_temp_lag2,
  ROUND(diff_temp_lag3, 1) AS diff_temp_lag3,
  ROUND(diff_temp_lag4, 1) AS diff_temp_lag4,
  ROUND(diff_temp_lag5, 1) AS diff_temp_lag5,
  ROUND(diff_temp_lag6, 1) AS diff_temp_lag6,
  ROUND(diff_temp_lag7, 1) AS diff_temp_lag7,
  ROUND(diff_temp_lag8, 1) AS diff_temp_lag8,
  ROUND(diff_prcp_lag1, 1) AS diff_prcp_lag1,
  ROUND(diff_prcp_lag2, 1) AS diff_prcp_lag2,
  ROUND(diff_prcp_lag3, 1) AS diff_prcp_lag3,
  ROUND(diff_prcp_lag4, 1) AS diff_prcp_lag4,
  ROUND(diff_prcp_lag5, 1) AS diff_prcp_lag5,
  ROUND(diff_prcp_lag6, 1) AS diff_prcp_lag6,
  ROUND(diff_prcp_lag7, 1) AS diff_prcp_lag7,
  ROUND(diff_prcp_lag8, 1) AS diff_prcp_lag8,
  ROUND(diff_wdsp_lag1, 1) AS diff_wdsp_lag1,
  ROUND(diff_wdsp_lag2, 1) AS diff_wdsp_lag2,
  ROUND(diff_wdsp_lag3, 1) AS diff_wdsp_lag3,
  ROUND(diff_wdsp_lag4, 1) AS diff_wdsp_lag4,
  ROUND(diff_wdsp_lag5, 1) AS diff_wdsp_lag5,
  ROUND(diff_wdsp_lag6, 1) AS diff_wdsp_lag6,
  ROUND(diff_wdsp_lag7, 1) AS diff_wdsp_lag7,
  ROUND(diff_wdsp_lag8, 1) AS diff_wdsp_lag8
FROM lag_diffs
WHERE "date" >= '2019-01-09'
ORDER BY "date"