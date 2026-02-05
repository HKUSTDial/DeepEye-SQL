WITH monthly_counts AS (
  SELECT 
    CAST(substr(insert_date, 1, 4) AS INTEGER) AS year,
    CAST(substr(insert_date, 6, 2) AS INTEGER) AS month,
    COUNT(*) AS monthly_count
  FROM `cities`
  WHERE CAST(substr(insert_date, 1, 4) AS INTEGER) BETWEEN 2021 AND 2023
    AND CAST(substr(insert_date, 6, 2) AS INTEGER) IN (4, 5, 6)
  GROUP BY year, month
),
with_running AS (
  SELECT 
    year,
    month,
    monthly_count,
    SUM(monthly_count) OVER (PARTITION BY month ORDER BY year) AS running_total
  FROM monthly_counts
),
with_prev AS (
  SELECT 
    year,
    month,
    monthly_count,
    running_total,
    LAG(monthly_count) OVER (PARTITION BY month ORDER BY year) AS prev_monthly_count,
    LAG(running_total) OVER (PARTITION BY month ORDER BY year) AS prev_running_total
  FROM with_running
),
with_growth AS (
  SELECT 
    year,
    month,
    monthly_count,
    running_total,
    CASE 
      WHEN prev_monthly_count IS NULL OR prev_monthly_count = 0 THEN NULL
      ELSE ROUND((monthly_count - prev_monthly_count) * 100.0 / prev_monthly_count, 2)
    END AS monthly_growth_pct,
    CASE 
      WHEN prev_running_total IS NULL OR prev_running_total = 0 THEN NULL
      ELSE ROUND((running_total - prev_running_total) * 100.0 / prev_running_total, 2)
    END AS running_growth_pct
  FROM with_prev
)
SELECT 
  year,
  month,
  monthly_count,
  running_total,
  monthly_growth_pct,
  running_growth_pct
FROM with_growth
WHERE year IN (2022, 2023)
ORDER BY year, month;