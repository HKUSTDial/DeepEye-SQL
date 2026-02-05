WITH track_stats AS (
  SELECT 
    MIN("Milliseconds") AS min_ms,
    AVG("Milliseconds") AS avg_ms,
    MAX("Milliseconds") AS max_ms
  FROM "MUSIC"."MUSIC"."TRACK"
),
boundaries AS (
  SELECT 
    min_ms,
    avg_ms,
    max_ms,
    (min_ms + avg_ms) / 2 AS mid_min_avg,
    (avg_ms + max_ms) / 2 AS mid_avg_max
  FROM track_stats
),
track_categories AS (
  SELECT 
    t."TrackId",
    t."Milliseconds",
    CASE 
      WHEN t."Milliseconds" < b.mid_min_avg THEN 'Short'
      WHEN t."Milliseconds" < b.mid_avg_max THEN 'Medium'
      ELSE 'Long'
    END AS category
  FROM "MUSIC"."MUSIC"."TRACK" t
  CROSS JOIN boundaries b
),
revenue_per_track AS (
  SELECT 
    "TrackId",
    SUM("UnitPrice" * "Quantity") AS track_revenue
  FROM "MUSIC"."MUSIC"."INVOICELINE"
  GROUP BY "TrackId"
)
SELECT 
  tc.category,
  MIN(tc."Milliseconds" / 60000.0) AS min_duration_minutes,
  MAX(tc."Milliseconds" / 60000.0) AS max_duration_minutes,
  COALESCE(SUM(rpt.track_revenue), 0) AS total_revenue
FROM track_categories tc
LEFT JOIN revenue_per_track rpt ON tc."TrackId" = rpt."TrackId"
GROUP BY tc.category
ORDER BY tc.category