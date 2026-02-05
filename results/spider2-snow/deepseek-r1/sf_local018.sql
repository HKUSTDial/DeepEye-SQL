WITH collisions_filtered AS (
  SELECT 
    "pcf_violation_category",
    EXTRACT(YEAR FROM TO_DATE("collision_date", 'YYYY-MM-DD')) AS year
  FROM "CALIFORNIA_TRAFFIC_COLLISION"."CALIFORNIA_TRAFFIC_COLLISION"."COLLISIONS"
  WHERE EXTRACT(YEAR FROM TO_DATE("collision_date", 'YYYY-MM-DD')) IN (2011, 2021)
),
top_category AS (
  SELECT 
    "pcf_violation_category"
  FROM collisions_filtered
  WHERE year = 2021
  GROUP BY "pcf_violation_category"
  ORDER BY COUNT(*) DESC
  LIMIT 1
),
shares AS (
  SELECT 
    year,
    (COUNT(CASE WHEN "pcf_violation_category" = (SELECT "pcf_violation_category" FROM top_category) THEN 1 END) * 100.0 / COUNT(*)) AS share
  FROM collisions_filtered
  GROUP BY year
)
SELECT 
  (SELECT share FROM shares WHERE year = 2011) - (SELECT share FROM shares WHERE year = 2021) AS percentage_points_decrease