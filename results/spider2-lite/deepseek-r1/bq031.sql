WITH rochester_stations AS (
  SELECT usaf, wban
  FROM `bigquery-public-data.noaa_gsod.stations`
  WHERE UPPER(TRIM(name)) LIKE '%ROCHESTER%'
),
daily_data AS (
  SELECT 
    DATE(CONCAT(s.year, '-', s.mo, '-', s.da)) as date,
    AVG((s.temp - 32) * 5/9) as temp_c,
    AVG(s.prcp * 2.54) as prcp_cm,
    AVG(CAST(s.wdsp AS FLOAT64) * 0.514444) as wdsp_ms
  FROM `bigquery-public-data.noaa_gsod.gsod2019` s
  INNER JOIN rochester_stations rs ON s.stn = rs.usaf AND s.wban = rs.wban
  WHERE 
    s.temp != 9999.9
    AND s.prcp != 99.99
    AND s.wdsp != '999.9'
    AND DATE(CONCAT(s.year, '-', s.mo, '-', s.da)) BETWEEN DATE '2019-01-01' AND DATE '2019-03-31'
  GROUP BY date
),
moving_avgs AS (
  SELECT 
    date,
    ROUND(temp_c, 1) as temp_daily,
    ROUND(prcp_cm, 1) as prcp_daily,
    ROUND(wdsp_ms, 1) as wdsp_daily,
    ROUND(AVG(temp_c) OVER (ORDER BY date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), 1) as temp_ma,
    ROUND(AVG(prcp_cm) OVER (ORDER BY date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), 1) as prcp_ma,
    ROUND(AVG(wdsp_ms) OVER (ORDER BY date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), 1) as wdsp_ma
  FROM daily_data
),
with_lags AS (
  SELECT *,
    ROUND(temp_ma - LAG(temp_ma, 1) OVER (ORDER BY date), 1) as temp_ma_diff1,
    ROUND(temp_ma - LAG(temp_ma, 2) OVER (ORDER BY date), 1) as temp_ma_diff2,
    ROUND(temp_ma - LAG(temp_ma, 3) OVER (ORDER BY date), 1) as temp_ma_diff3,
    ROUND(temp_ma - LAG(temp_ma, 4) OVER (ORDER BY date), 1) as temp_ma_diff4,
    ROUND(temp_ma - LAG(temp_ma, 5) OVER (ORDER BY date), 1) as temp_ma_diff5,
    ROUND(temp_ma - LAG(temp_ma, 6) OVER (ORDER BY date), 1) as temp_ma_diff6,
    ROUND(temp_ma - LAG(temp_ma, 7) OVER (ORDER BY date), 1) as temp_ma_diff7,
    ROUND(temp_ma - LAG(temp_ma, 8) OVER (ORDER BY date), 1) as temp_ma_diff8,
    ROUND(prcp_ma - LAG(prcp_ma, 1) OVER (ORDER BY date), 1) as prcp_ma_diff1,
    ROUND(prcp_ma - LAG(prcp_ma, 2) OVER (ORDER BY date), 1) as prcp_ma_diff2,
    ROUND(prcp_ma - LAG(prcp_ma, 3) OVER (ORDER BY date), 1) as prcp_ma_diff3,
    ROUND(prcp_ma - LAG(prcp_ma, 4) OVER (ORDER BY date), 1) as prcp_ma_diff4,
    ROUND(prcp_ma - LAG(prcp_ma, 5) OVER (ORDER BY date), 1) as prcp_ma_diff5,
    ROUND(prcp_ma - LAG(prcp_ma, 6) OVER (ORDER BY date), 1) as prcp_ma_diff6,
    ROUND(prcp_ma - LAG(prcp_ma, 7) OVER (ORDER BY date), 1) as prcp_ma_diff7,
    ROUND(prcp_ma - LAG(prcp_ma, 8) OVER (ORDER BY date), 1) as prcp_ma_diff8,
    ROUND(wdsp_ma - LAG(wdsp_ma, 1) OVER (ORDER BY date), 1) as wdsp_ma_diff1,
    ROUND(wdsp_ma - LAG(wdsp_ma, 2) OVER (ORDER BY date), 1) as wdsp_ma_diff2,
    ROUND(wdsp_ma - LAG(wdsp_ma, 3) OVER (ORDER BY date), 1) as wdsp_ma_diff3,
    ROUND(wdsp_ma - LAG(wdsp_ma, 4) OVER (ORDER BY date), 1) as wdsp_ma_diff4,
    ROUND(wdsp_ma - LAG(wdsp_ma, 5) OVER (ORDER BY date), 1) as wdsp_ma_diff5,
    ROUND(wdsp_ma - LAG(wdsp_ma, 6) OVER (ORDER BY date), 1) as wdsp_ma_diff6,
    ROUND(wdsp_ma - LAG(wdsp_ma, 7) OVER (ORDER BY date), 1) as wdsp_ma_diff7,
    ROUND(wdsp_ma - LAG(wdsp_ma, 8) OVER (ORDER BY date), 1) as wdsp_ma_diff8
  FROM moving_avgs
)
SELECT *
FROM with_lags
WHERE date >= DATE '2019-01-09'
ORDER BY date;