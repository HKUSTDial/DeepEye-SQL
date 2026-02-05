WITH all_dates AS (
  SELECT date
  FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01', '2016-12-31')) AS date
),
incident_counts AS (
  SELECT `date`, COUNT(*) AS count
  FROM `bigquery-public-data.austin_incidents.incidents_2016`
  WHERE descript = 'PUBLIC INTOXICATION'
  GROUP BY `date`
),
daily_counts AS (
  SELECT all_dates.date, COALESCE(incident_counts.count, 0) AS count
  FROM all_dates
  LEFT JOIN incident_counts ON all_dates.date = incident_counts.date
),
stats AS (
  SELECT AVG(count) AS mean, STDDEV(count) AS std
  FROM daily_counts
),
z_scores AS (
  SELECT date, (count - mean) / std AS z
  FROM daily_counts
  CROSS JOIN stats
),
ranked AS (
  SELECT date, z, RANK() OVER (ORDER BY z DESC) AS rnk
  FROM z_scores
)
SELECT FORMAT_DATE('%Y-%m-%d', date) AS formatted_date
FROM ranked
WHERE rnk = 2