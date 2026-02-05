WITH events_past_15_years AS (
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2009" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2010" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2011" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2012" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2013" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2014" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2015" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2016" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2017" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2018" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2019" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2020" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2021" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2022" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2023" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
    UNION ALL
    SELECT "event_id", "event_begin_time", "damage_property" FROM "NOAA_DATA"."NOAA_HISTORIC_SEVERE_STORMS"."STORMS_2024" WHERE TO_TIMESTAMP("event_begin_time" / 1000000) >= DATEADD(year, -15, CURRENT_DATE())
),
top_100_events AS (
    SELECT "event_id", "event_begin_time", "damage_property", DATE_TRUNC('month', TO_TIMESTAMP("event_begin_time" / 1000000)) AS event_month
    FROM events_past_15_years
    ORDER BY "damage_property" DESC NULLS LAST
    LIMIT 100
)
SELECT COUNT(*) AS total_events_in_most_affected_month
FROM top_100_events
GROUP BY event_month
ORDER BY COUNT(*) DESC
LIMIT 1