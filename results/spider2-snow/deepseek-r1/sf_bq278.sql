WITH postal_aggregates AS (
  SELECT 
    "state_name",
    SUM("count_qualified") AS "total_buildings_postal",
    AVG("percent_covered") AS "avg_percent_covered_postal",
    AVG("percent_qualified") AS "avg_percent_qualified_postal",
    SUM("number_of_panels_total") AS "total_panels_postal",
    SUM("kw_total") AS "total_kw_postal",
    SUM("yearly_sunlight_kwh_total") AS "total_energy_postal",
    SUM("carbon_offset_metric_tons") AS "total_carbon_offset_postal",
    SUM("existing_installs_count") AS "current_installations_postal",
    (SUM("count_qualified") * (AVG("percent_covered")/100) * (AVG("percent_qualified")/100)) - SUM("existing_installs_count") AS "gap_postal"
  FROM "SUNROOF_SOLAR"."SUNROOF_SOLAR"."SOLAR_POTENTIAL_BY_POSTAL_CODE"
  GROUP BY "state_name"
),
census_aggregates AS (
  SELECT 
    "state_name",
    SUM("count_qualified") AS "total_buildings_census",
    AVG("percent_covered") AS "avg_percent_covered_census",
    AVG("percent_qualified") AS "avg_percent_qualified_census",
    SUM("number_of_panels_total") AS "total_panels_census",
    SUM("kw_total") AS "total_kw_census",
    SUM("yearly_sunlight_kwh_total") AS "total_energy_census",
    SUM("carbon_offset_metric_tons") AS "total_carbon_offset_census",
    SUM("existing_installs_count") AS "current_installations_census",
    (SUM("count_qualified") * (AVG("percent_covered")/100) * (AVG("percent_qualified")/100)) - SUM("existing_installs_count") AS "gap_census"
  FROM "SUNROOF_SOLAR"."SUNROOF_SOLAR"."SOLAR_POTENTIAL_BY_CENSUSTRACT"
  GROUP BY "state_name"
)
SELECT 
  COALESCE(p."state_name", c."state_name") AS "state_name",
  p."total_buildings_postal",
  p."avg_percent_covered_postal",
  p."avg_percent_qualified_postal",
  p."total_panels_postal",
  p."total_kw_postal",
  p."total_energy_postal",
  p."total_carbon_offset_postal",
  p."current_installations_postal",
  p."gap_postal",
  c."total_buildings_census",
  c."avg_percent_covered_census",
  c."avg_percent_qualified_census",
  c."total_panels_census",
  c."total_kw_census",
  c."total_energy_census",
  c."total_carbon_offset_census",
  c."current_installations_census",
  c."gap_census"
FROM postal_aggregates p
FULL OUTER JOIN census_aggregates c ON p."state_name" = c."state_name"
ORDER BY COALESCE(p."state_name", c."state_name")