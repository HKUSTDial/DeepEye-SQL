WITH cause_counts AS (
  SELECT 
    ci.db_year,
    c.pcf_violation_category,
    COUNT(*) as count
  FROM collisions c
  JOIN case_ids ci ON c.case_id = ci.case_id
  WHERE c.pcf_violation_category IS NOT NULL
  GROUP BY ci.db_year, c.pcf_violation_category
),
ranked_causes AS (
  SELECT 
    db_year,
    pcf_violation_category,
    count,
    ROW_NUMBER() OVER (PARTITION BY db_year ORDER BY count DESC) as rank
  FROM cause_counts
),
top_two_per_year AS (
  SELECT db_year, pcf_violation_category
  FROM ranked_causes
  WHERE rank <= 2
),
aggregated AS (
  SELECT 
    db_year,
    GROUP_CONCAT(pcf_violation_category ORDER BY pcf_violation_category) as top_two_set
  FROM top_two_per_year
  GROUP BY db_year
),
set_counts AS (
  SELECT top_two_set, COUNT(*) as num_years
  FROM aggregated
  GROUP BY top_two_set
)
SELECT a.db_year
FROM aggregated a
JOIN set_counts s ON a.top_two_set = s.top_two_set
WHERE s.num_years = 1