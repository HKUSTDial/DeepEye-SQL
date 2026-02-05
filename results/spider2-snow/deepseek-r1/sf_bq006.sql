WITH daily_counts AS (
  SELECT "date", COUNT(*) AS count
  FROM "AUSTIN"."AUSTIN_INCIDENTS"."INCIDENTS_2016"
  WHERE "descript" = 'PUBLIC INTOXICATION'
  GROUP BY "date"
),
all_dates AS (
  SELECT DATEADD(day, seq4(), '2016-01-01'::DATE) AS "date"
  FROM TABLE(GENERATOR(ROWCOUNT => 366))
  WHERE "date" <= '2016-12-31'::DATE
),
daily_counts_with_zero AS (
  SELECT 
    ad."date",
    COALESCE(dc.count, 0) AS count
  FROM all_dates ad
  LEFT JOIN daily_counts dc ON ad."date" = dc."date"
),
stats AS (
  SELECT
    AVG(count) AS avg_count,
    STDDEV_POP(count) AS stddev_count
  FROM daily_counts_with_zero
),
z_scores AS (
  SELECT
    dc."date",
    (dc.count - s.avg_count) / NULLIF(s.stddev_count, 0) AS z_score
  FROM daily_counts_with_zero dc
  CROSS JOIN stats s
),
ranked AS (
  SELECT
    "date",
    z_score,
    DENSE_RANK() OVER (ORDER BY z_score DESC) AS rnk
  FROM z_scores
)
SELECT TO_CHAR("date", 'YYYY-MM-DD') AS "date"
FROM ranked
WHERE rnk = 2
ORDER BY "date"
LIMIT 1