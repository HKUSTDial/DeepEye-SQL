WITH corn_data AS (
  SELECT "state_name", SUM("value") AS corn_production
  FROM "USDA_NASS_AGRICULTURE"."USDA_NASS_AGRICULTURE"."CROPS"
  WHERE "year" = 2022
    AND "commodity_desc" = 'CORN'
    AND "group_desc" = 'FIELD CROPS'
    AND "statisticcat_desc" = 'PRODUCTION'
    AND "agg_level_desc" = 'STATE'
    AND "unit_desc" = 'BU'
    AND "value" IS NOT NULL
  GROUP BY "state_name"
),
mushroom_data AS (
  SELECT "state_name", SUM("value") AS mushroom_production
  FROM "USDA_NASS_AGRICULTURE"."USDA_NASS_AGRICULTURE"."CROPS"
  WHERE "year" = 2022
    AND "commodity_desc" = 'MUSHROOMS'
    AND "group_desc" = 'HORTICULTURE'
    AND "statisticcat_desc" = 'PRODUCTION'
    AND "agg_level_desc" = 'STATE'
    AND "value" IS NOT NULL
  GROUP BY "state_name"
)
SELECT 
  COALESCE(c."state_name", m."state_name") AS "state_name",
  COALESCE(c.corn_production, 0) AS corn_production,
  COALESCE(m.mushroom_production, 0) AS mushroom_production
FROM corn_data c
FULL OUTER JOIN mushroom_data m ON c."state_name" = m."state_name"
ORDER BY "state_name"