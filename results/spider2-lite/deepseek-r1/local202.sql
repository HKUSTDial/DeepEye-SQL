WITH state_stats AS (
    SELECT state,
           COUNT(*) AS population,
           AVG(age) AS avg_age,
           SUM(CASE WHEN aggressive = 0 THEN 1 ELSE 0 END) AS friendly_count,
           SUM(CASE WHEN aggressive = 1 THEN 1 ELSE 0 END) AS hostile_count
    FROM alien_data
    GROUP BY state
),
top_states AS (
    SELECT state, population, avg_age, friendly_count, hostile_count
    FROM state_stats
    ORDER BY population DESC
    LIMIT 10
)
SELECT COUNT(*) AS answer
FROM top_states
WHERE friendly_count > hostile_count AND avg_age > 200;