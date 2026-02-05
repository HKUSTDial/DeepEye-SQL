WITH per_state_stats AS (
  SELECT "state", 
         COUNT(*) as total_aliens,
         SUM(CASE WHEN "aggressive" = 0 THEN 1 ELSE 0 END) as friendly_count,
         SUM(CASE WHEN "aggressive" = 1 THEN 1 ELSE 0 END) as hostile_count,
         AVG("age") as avg_age
  FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."ALIEN_DATA"
  GROUP BY "state"
),
top10_states AS (
  SELECT "state", total_aliens, friendly_count, hostile_count, avg_age
  FROM per_state_stats
  ORDER BY total_aliens DESC
  LIMIT 10
)
SELECT COUNT(*) as answer
FROM top10_states
WHERE friendly_count > hostile_count AND avg_age > 200