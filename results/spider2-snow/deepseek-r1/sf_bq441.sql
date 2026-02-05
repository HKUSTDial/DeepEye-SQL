SELECT
  a."consecutive_number",
  a."county",
  a."type_of_intersection",
  a."light_condition",
  a."atmospheric_conditions_1",
  a."hour_of_crash",
  a."functional_system",
  a."related_factors_crash_level_1" AS "related_factors",
  CASE WHEN a."hour_of_ems_arrival_at_hospital" BETWEEN 0 AND 23 THEN a."hour_of_ems_arrival_at_hospital" - a."hour_of_crash" ELSE NULL END AS "delay_to_hospital",
  CASE WHEN a."hour_of_arrival_at_scene" BETWEEN 0 AND 23 THEN a."hour_of_arrival_at_scene" - a."hour_of_crash" ELSE NULL END AS "delay_to_scene",
  p."age",
  p."person_type",
  p."seating_position",
  CASE p."restraint_system_helmet_use"
    WHEN 0 THEN 0
    WHEN 1 THEN 0.33
    WHEN 2 THEN 0.67
    WHEN 3 THEN 1.0
    ELSE 0.5
  END AS "restraint",
  CASE WHEN p."injury_severity" = 4 THEN 1 ELSE 0 END AS "survived",
  CASE WHEN p."rollover" LIKE '%Rollover%' THEN 1 ELSE 0 END AS "rollover",
  CASE WHEN p."air_bag_deployed" BETWEEN 1 AND 9 THEN 1 ELSE 0 END AS "airbag",
  CASE WHEN p."police_reported_alcohol_involvement" LIKE '%Yes%' THEN 1 ELSE 0 END AS "alcohol",
  CASE WHEN p."police_reported_drug_involvement" LIKE '%Yes%' THEN 1 ELSE 0 END AS "drugs",
  p."related_factors_person_level1",
  v."travel_speed",
  CASE WHEN v."speeding_related" LIKE '%Yes%' THEN 1 ELSE 0 END AS "speeding_related",
  v."extent_of_damage",
  v."body_type",
  v."vehicle_removal",
  CASE WHEN v."manner_of_collision" > 11 THEN 11 ELSE v."manner_of_collision" END AS "manner_of_collision",
  CASE WHEN v."roadway_surface_condition" > 11 THEN 8 ELSE v."roadway_surface_condition" END AS "roadway_surface_condition",
  CASE WHEN v."first_harmful_event" < 90 THEN v."first_harmful_event" ELSE 0 END AS "first_harmful_event",
  CASE WHEN v."most_harmful_event" < 90 THEN v."most_harmful_event" ELSE 0 END AS "most_harmful_event"
FROM "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."ACCIDENT_2015" a
INNER JOIN "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."PERSON_2015" p
  ON a."consecutive_number" = p."consecutive_number"
  AND a."state_number" = p."state_number"
LEFT JOIN "NHTSA_TRAFFIC_FATALITIES"."NHTSA_TRAFFIC_FATALITIES"."VEHICLE_2015" v
  ON p."consecutive_number" = v."consecutive_number"
  AND p."state_number" = v."state_number"
  AND p."vehicle_number" = v."vehicle_number"