WITH all_events AS (
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180726"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180715"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180805"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180707"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180816"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180725"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180708"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180802"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180823"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180703"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180809"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180811"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180730"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180727"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180821"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180731"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180702"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180818"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180718"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180824"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180723"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180714"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180803"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180728"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180822"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180819"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180720"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180722"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180826"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180710"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180724"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180814"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180705"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180804"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180825"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180813"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180815"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180806"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180704"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180817"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180712"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180717"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180812"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180807"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180810"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180716"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180801"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180820"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180713"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180706"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180709"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180729"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180721"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180808"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180827"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180711"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180719"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180918"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180614"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180630"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20181002"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180926"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180830"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180907"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180829"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180914"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180902"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180831"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180910"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20181003"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180908"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180915"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180905"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180906"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180909"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180916"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180925"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180901"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180928"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180917"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20181001"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180828"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180912"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180903"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180930"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180920"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180924"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180919"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180913"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180922"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180904"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180921"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180911"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180927"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180923"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180929"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180619"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180615"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180625"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180626"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180618"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180620"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180621"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180612"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180622"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180617"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180613"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180624"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180623"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180616"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180701"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180627"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180629"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
  UNION ALL
  SELECT 
    "user_pseudo_id",
    TO_DATE("event_date", 'YYYYMMDD') AS event_date_parsed,
    "event_timestamp",
    CAST(NULL AS NUMBER) AS "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180628"
  WHERE TO_DATE("event_date", 'YYYYMMDD') <= DATE '2018-10-02'
),
user_first_session AS (
  SELECT 
    "user_pseudo_id",
    MIN(COALESCE("user_first_touch_timestamp", "event_timestamp")) AS first_timestamp
  FROM all_events
  GROUP BY "user_pseudo_id"
),
cohort AS (
  SELECT 
    "user_pseudo_id",
    DATE_TRUNC('WEEK', TO_TIMESTAMP(first_timestamp / 1000000)) AS cohort_week
  FROM user_first_session
  WHERE DATE_TRUNC('WEEK', TO_TIMESTAMP(first_timestamp / 1000000)) = DATE '2018-07-02'
),
user_events AS (
  SELECT 
    c."user_pseudo_id",
    c.cohort_week,
    ae.event_date_parsed,
    FLOOR(DATEDIFF('day', c.cohort_week, ae.event_date_parsed) / 7) AS week_index
  FROM cohort c
  JOIN all_events ae ON c."user_pseudo_id" = ae."user_pseudo_id"
  WHERE ae.event_date_parsed <= DATE '2018-10-02'
    AND ae.event_date_parsed >= c.cohort_week
    AND week_index BETWEEN 0 AND 4
),
retention_counts AS (
  SELECT 
    "user_pseudo_id",
    cohort_week,
    MAX(CASE WHEN week_index = 0 THEN 1 ELSE 0 END) AS week_0,
    MAX(CASE WHEN week_index = 1 THEN 1 ELSE 0 END) AS week_1,
    MAX(CASE WHEN week_index = 2 THEN 1 ELSE 0 END) AS week_2,
    MAX(CASE WHEN week_index = 3 THEN 1 ELSE 0 END) AS week_3,
    MAX(CASE WHEN week_index = 4 THEN 1 ELSE 0 END) AS week_4
  FROM user_events
  GROUP BY "user_pseudo_id", cohort_week
)
SELECT 
  COUNT(DISTINCT "user_pseudo_id") AS total_users_week0,
  COUNT(DISTINCT CASE WHEN week_1 = 1 THEN "user_pseudo_id" END) AS retained_week1,
  COUNT(DISTINCT CASE WHEN week_2 = 1 THEN "user_pseudo_id" END) AS retained_week2,
  COUNT(DISTINCT CASE WHEN week_3 = 1 THEN "user_pseudo_id" END) AS retained_week3,
  COUNT(DISTINCT CASE WHEN week_4 = 1 THEN "user_pseudo_id" END) AS retained_week4
FROM retention_counts