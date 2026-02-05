SELECT 
  a.`consecutive_number`,
  a.`county` AS `county`,
  a.`type_of_intersection`,
  a.`light_condition`,
  a.`atmospheric_conditions_1`,
  a.`hour_of_crash`,
  a.`functional_system`,
  a.`related_factors_crash_level_1` AS `related_factors`,
  CASE 
    WHEN a.`hour_of_ems_arrival_at_hospital` BETWEEN 0 AND 23 
    THEN a.`hour_of_ems_arrival_at_hospital` - a.`hour_of_crash`
    ELSE NULL 
  END AS `delay_to_hospital`,
  CASE 
    WHEN a.`hour_of_arrival_at_scene` BETWEEN 0 AND 23 
    THEN a.`hour_of_arrival_at_scene` - a.`hour_of_crash`
    ELSE NULL 
  END AS `delay_to_scene`,
  p.`age`,
  p.`person_type`,
  p.`seating_position`,
  CASE 
    WHEN p.`restraint_system_helmet_use` = 0 THEN 0.0
    WHEN p.`restraint_system_helmet_use` = 1 THEN 0.33
    WHEN p.`restraint_system_helmet_use` = 2 THEN 0.67
    WHEN p.`restraint_system_helmet_use` = 3 THEN 1.0
    ELSE 0.5 
  END AS `restraint`,
  CASE 
    WHEN p.`injury_severity` = 4 THEN 1 
    ELSE 0 
  END AS `survived`,
  CASE 
    WHEN p.`rollover` = 'No Rollover' THEN 0 
    ELSE 1 
  END AS `rollover`,
  CASE 
    WHEN p.`air_bag_deployed` BETWEEN 1 AND 9 THEN 1 
    ELSE 0 
  END AS `airbag`,
  CASE 
    WHEN p.`police_reported_alcohol_involvement` LIKE '%Yes%' THEN 1 
    ELSE 0 
  END AS `alcohol`,
  CASE 
    WHEN p.`police_reported_drug_involvement` LIKE '%Yes%' THEN 1 
    ELSE 0 
  END AS `drugs`,
  p.`related_factors_person_level1`,
  v.`travel_speed`,
  CASE 
    WHEN v.`speeding_related` LIKE '%Yes%' THEN 1 
    ELSE 0 
  END AS `speeding_related`,
  v.`extent_of_damage`,
  v.`body_type`,
  v.`vehicle_removal`,
  CASE 
    WHEN a.`manner_of_collision` > 11 THEN 11 
    ELSE a.`manner_of_collision` 
  END AS `manner_of_collision`,
  CASE 
    WHEN v.`roadway_surface_condition` > 11 THEN 8 
    ELSE v.`roadway_surface_condition` 
  END AS `roadway_surface_condition`,
  CASE 
    WHEN a.`first_harmful_event` < 90 THEN a.`first_harmful_event` 
    ELSE 0 
  END AS `first_harmful_event`,
  CASE 
    WHEN v.`most_harmful_event` < 90 THEN v.`most_harmful_event` 
    ELSE 0 
  END AS `most_harmful_event`
FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015` a
LEFT JOIN `bigquery-public-data.nhtsa_traffic_fatalities.vehicle_2015` v
  ON a.`consecutive_number` = v.`consecutive_number`
LEFT JOIN `bigquery-public-data.nhtsa_traffic_fatalities.person_2015` p
  ON v.`consecutive_number` = p.`consecutive_number`
  AND v.`vehicle_number` = p.`vehicle_number`
WHERE EXTRACT(YEAR FROM a.`timestamp_of_crash`) = 2015