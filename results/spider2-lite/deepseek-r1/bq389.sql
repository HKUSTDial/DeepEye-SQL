WITH pm10_data AS (
  SELECT 
    EXTRACT(MONTH FROM date_local) AS month,
    AVG(arithmetic_mean) AS pm10_avg
  FROM `bigquery-public-data.epa_historical_air_quality.pm10_daily_summary`
  WHERE state_name = 'California'
    AND EXTRACT(YEAR FROM date_local) = 2020
  GROUP BY EXTRACT(MONTH FROM date_local)
),
pm25_frm_data AS (
  SELECT 
    EXTRACT(MONTH FROM date_local) AS month,
    AVG(arithmetic_mean) AS pm25_frm_avg
  FROM `bigquery-public-data.epa_historical_air_quality.pm25_frm_daily_summary`
  WHERE state_name = 'California'
    AND EXTRACT(YEAR FROM date_local) = 2020
  GROUP BY EXTRACT(MONTH FROM date_local)
),
pm25_nonfrm_data AS (
  SELECT 
    EXTRACT(MONTH FROM date_local) AS month,
    AVG(arithmetic_mean) AS pm25_nonfrm_avg
  FROM `bigquery-public-data.epa_historical_air_quality.pm25_nonfrm_daily_summary`
  WHERE state_name = 'California'
    AND EXTRACT(YEAR FROM date_local) = 2020
  GROUP BY EXTRACT(MONTH FROM date_local)
),
voc_data AS (
  SELECT 
    EXTRACT(MONTH FROM date_local) AS month,
    AVG(arithmetic_mean) AS voc_avg
  FROM `bigquery-public-data.epa_historical_air_quality.voc_daily_summary`
  WHERE state_name = 'California'
    AND EXTRACT(YEAR FROM date_local) = 2020
  GROUP BY EXTRACT(MONTH FROM date_local)
),
so2_data AS (
  SELECT 
    EXTRACT(MONTH FROM date_local) AS month,
    AVG(arithmetic_mean) * 10 AS so2_avg
  FROM `bigquery-public-data.epa_historical_air_quality.so2_daily_summary`
  WHERE state_name = 'California'
    AND EXTRACT(YEAR FROM date_local) = 2020
  GROUP BY EXTRACT(MONTH FROM date_local)
),
lead_data AS (
  SELECT 
    EXTRACT(MONTH FROM date_local) AS month,
    AVG(arithmetic_mean) * 100 AS lead_avg
  FROM `bigquery-public-data.epa_historical_air_quality.lead_daily_summary`
  WHERE state_name = 'California'
    AND EXTRACT(YEAR FROM date_local) = 2020
  GROUP BY EXTRACT(MONTH FROM date_local)
)
SELECT 
  COALESCE(pm10.month, pm25_frm.month, pm25_nonfrm.month, voc.month, so2.month, lead.month) AS month,
  pm10_avg,
  pm25_frm_avg,
  pm25_nonfrm_avg,
  voc_avg,
  so2_avg,
  lead_avg
FROM pm10_data pm10
FULL OUTER JOIN pm25_frm_data pm25_frm ON pm10.month = pm25_frm.month
FULL OUTER JOIN pm25_nonfrm_data pm25_nonfrm ON COALESCE(pm10.month, pm25_frm.month) = pm25_nonfrm.month
FULL OUTER JOIN voc_data voc ON COALESCE(pm10.month, pm25_frm.month, pm25_nonfrm.month) = voc.month
FULL OUTER JOIN so2_data so2 ON COALESCE(pm10.month, pm25_frm.month, pm25_nonfrm.month, voc.month) = so2.month
FULL OUTER JOIN lead_data lead ON COALESCE(pm10.month, pm25_frm.month, pm25_nonfrm.month, voc.month, so2.month) = lead.month
ORDER BY month