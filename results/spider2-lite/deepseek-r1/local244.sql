WITH stats AS (
    SELECT MIN(Milliseconds) AS min_ms,
           AVG(Milliseconds) AS avg_ms,
           MAX(Milliseconds) AS max_ms
    FROM Track
),
midpoints AS (
    SELECT min_ms,
           avg_ms,
           max_ms,
           (min_ms + avg_ms) / 2.0 AS mid1,
           (avg_ms + max_ms) / 2.0 AS mid2
    FROM stats
),
track_category AS (
    SELECT t.TrackId,
           t.Milliseconds,
           CASE
               WHEN t.Milliseconds < m.mid1 THEN 'short'
               WHEN t.Milliseconds < m.mid2 THEN 'medium'
               ELSE 'long'
           END AS category
    FROM Track t
    CROSS JOIN midpoints m
),
track_revenue AS (
    SELECT TrackId,
           SUM(Quantity * UnitPrice) AS revenue
    FROM InvoiceLine
    GROUP BY TrackId
)
SELECT category,
       MIN(Milliseconds * 1.0 / 60000) AS min_time_minutes,
       MAX(Milliseconds * 1.0 / 60000) AS max_time_minutes,
       SUM(COALESCE(tr.revenue, 0)) AS total_revenue
FROM track_category tc
LEFT JOIN track_revenue tr ON tc.TrackId = tr.TrackId
GROUP BY category
ORDER BY category;