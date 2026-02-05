WITH date_data AS (
  SELECT 
    calendar_year,
    sales,
    julianday(week_date) - julianday(calendar_year || '-06-15') AS days_from_june15
  FROM cleaned_weekly_sales
  WHERE calendar_year IN (2018, 2019, 2020)
),
categorized_sales AS (
  SELECT
    calendar_year,
    CASE 
      WHEN days_from_june15 BETWEEN -28 AND -1 THEN 'before'
      WHEN days_from_june15 BETWEEN 1 AND 28 THEN 'after'
    END AS period,
    SUM(sales) AS total_sales
  FROM date_data
  WHERE days_from_june15 BETWEEN -28 AND 28
    AND days_from_june15 != 0
  GROUP BY calendar_year, period
),
pivoted AS (
  SELECT
    calendar_year,
    SUM(CASE WHEN period = 'before' THEN total_sales END) AS before_sales,
    SUM(CASE WHEN period = 'after' THEN total_sales END) AS after_sales
  FROM categorized_sales
  GROUP BY calendar_year
)
SELECT
  calendar_year,
  before_sales,
  after_sales,
  ((after_sales - before_sales) * 100.0 / before_sales) AS percentage_change
FROM pivoted
ORDER BY calendar_year