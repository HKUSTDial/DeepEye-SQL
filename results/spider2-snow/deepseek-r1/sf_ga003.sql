WITH event_params_extracted AS (
  SELECT 
    e."user_pseudo_id",
    e."event_timestamp",
    MAX(CASE WHEN ep.value:key::string = 'board_type' THEN ep.value:value:string_value::string END) AS "board_type",
    MAX(CASE WHEN ep.value:key::string = 'score' THEN COALESCE(ep.value:value:int_value::DOUBLE, ep.value:value:double_value::DOUBLE) END) AS "score"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180915" AS e,
  LATERAL FLATTEN(INPUT => e."event_params") AS ep
  WHERE e."event_name" = 'level_end_quickplay'
    AND e."event_date" = '20180915'
  GROUP BY e."user_pseudo_id", e."event_timestamp"
  HAVING "board_type" IS NOT NULL AND "score" IS NOT NULL
)
SELECT 
  "board_type",
  AVG("score") AS "average_score"
FROM event_params_extracted
GROUP BY "board_type"
ORDER BY "average_score" DESC