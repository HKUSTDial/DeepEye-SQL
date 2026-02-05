WITH helmet_cases AS (
  SELECT DISTINCT `case_id`
  FROM `parties`
  WHERE (`party_safety_equipment_1` LIKE '%motorcycle helmet used%' OR `party_safety_equipment_2` LIKE '%motorcycle helmet used%')
    AND `party_type` IN ('driver', 'passenger')
)
SELECT 
  CASE 
    WHEN h.`case_id` IS NOT NULL THEN 'With Helmet'
    ELSE 'Without Helmet'
  END AS helmet_category,
  SUM(c.`motorcyclist_killed_count`) AS total_fatalities,
  COUNT(DISTINCT c.`case_id`) AS total_collisions,
  ROUND(100.0 * SUM(c.`motorcyclist_killed_count`) / COUNT(DISTINCT c.`case_id`), 2) AS fatality_rate_percentage
FROM `collisions` c
LEFT JOIN helmet_cases h ON c.`case_id` = h.`case_id`
WHERE c.`motorcycle_collision` = 1
GROUP BY helmet_category;