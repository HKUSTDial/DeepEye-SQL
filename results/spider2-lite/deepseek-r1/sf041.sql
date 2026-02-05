WITH all_hours AS (
    SELECT 
        DATEADD('hour', seq4(), TIMESTAMP '2022-10-01 00:00:00') AS "DATETIME",
        'CDT' AS "TIMEZONE",
        CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD('hour', seq4(), TIMESTAMP '2022-10-01 00:00:00')) AS "DATETIME_UTC",
        'ERCOT' AS "ISO"
    FROM TABLE(GENERATOR(ROWCOUNT => 24))
),
time_periods AS (
    SELECT 
        ah."ISO",
        ah."DATETIME",
        ah."TIMEZONE",
        ah."DATETIME_UTC",
        imt."ONPEAK",
        imt."OFFPEAK",
        imt."WEPEAK",
        imt."WDPEAK",
        imt."MARKETDAY"
    FROM all_hours ah
    LEFT JOIN "YES_ENERGY__SAMPLE_DATA"."YES_ENERGY_SAMPLE"."ISO_MARKET_TIMES_SAMPLE" imt
        ON ah."DATETIME" = imt."DATETIME" AND ah."ISO" = imt."ISO"
),
price_data AS (
    SELECT 
        "DATETIME",
        "TIMEZONE",
        "DALMP",
        "RTLMP"
    FROM "YES_ENERGY__SAMPLE_DATA"."YES_ENERGY_SAMPLE"."DART_PRICES_SAMPLE"
    WHERE "OBJECTID" = 10000697078 AND "ISO" = 'E' AND DATE("DATETIME") = '2022-10-01' AND "TIMEZONE" = 'CDT'
),
load_forecast_data AS (
    SELECT 
        "DATETIME",
        "TIMEZONE",
        "VALUE" AS load_forecast,
        "PUBLISHDATE" AS load_forecast_publish_date
    FROM "YES_ENERGY__SAMPLE_DATA"."YES_ENERGY_SAMPLE"."TS_FORECAST_SAMPLE"
    WHERE "OBJECTID" = 10000712973 AND "DATATYPEID" = 19060 AND DATE("DATETIME") = '2022-10-01' AND "TIMEZONE" = 'CDT'
),
wind_forecast_data AS (
    SELECT 
        "DATETIME",
        "TIMEZONE",
        "VALUE" AS wind_gen_forecast,
        "PUBLISHDATE" AS wind_gen_forecast_publish_date
    FROM "YES_ENERGY__SAMPLE_DATA"."YES_ENERGY_SAMPLE"."TS_FORECAST_SAMPLE"
    WHERE "OBJECTID" = 10000712973 AND "DATATYPEID" = 9285 AND DATE("DATETIME") = '2022-10-01' AND "TIMEZONE" = 'CDT'
),
solar_forecast_data AS (
    SELECT 
        "DATETIME",
        "TIMEZONE",
        "VALUE" AS solar_gen_forecast,
        "PUBLISHDATE" AS solar_gen_forecast_publish_date
    FROM "YES_ENERGY__SAMPLE_DATA"."YES_ENERGY_SAMPLE"."TS_FORECAST_SAMPLE"
    WHERE "OBJECTID" = 10000712973 AND "DATATYPEID" = 662 AND DATE("DATETIME") = '2022-10-01' AND "TIMEZONE" = 'CDT'
),
actual_load_data AS (
    SELECT 
        "DATETIME",
        "TIMEZONE",
        "RTLOAD"
    FROM "YES_ENERGY__SAMPLE_DATA"."YES_ENERGY_SAMPLE"."DART_LOADS_SAMPLE"
    WHERE "OBJECTID" = 10000712973 AND DATE("DATETIME") = '2022-10-01' AND "TIMEZONE" = 'CDT'
),
actual_wind_gen AS (
    SELECT 
        "DATETIME",
        "TIMEZONE",
        "VALUE" AS wind_gen
    FROM "YES_ENERGY__SAMPLE_DATA"."YES_ENERGY_SAMPLE"."TS_GEN_SAMPLE"
    WHERE "OBJECTID" = 10000712973 AND "DATATYPEID" = 16 AND DATE("DATETIME") = '2022-10-01' AND "TIMEZONE" = 'CDT'
),
actual_solar_gen AS (
    SELECT 
        "DATETIME",
        "TIMEZONE",
        "VALUE" AS solar_gen
    FROM "YES_ENERGY__SAMPLE_DATA"."YES_ENERGY_SAMPLE"."TS_GEN_SAMPLE"
    WHERE "OBJECTID" = 10000712973 AND "DATATYPEID" = 650 AND DATE("DATETIME") = '2022-10-01' AND "TIMEZONE" = 'CDT'
)
SELECT 
    tp."ISO" AS iso,
    tp."DATETIME" AS datetime,
    tp."TIMEZONE" AS timezone,
    tp."DATETIME_UTC" AS datetime_utc,
    tp."ONPEAK" AS onpeak,
    tp."OFFPEAK" AS offpeak,
    tp."WEPEAK" AS wepeak,
    tp."WDPEAK" AS wdpeak,
    tp."MARKETDAY" AS marketday,
    'HB_NORTH' AS price_node_name,
    10000697078 AS price_node_id,
    pd."DALMP" AS dalmp,
    pd."RTLMP" AS rtlmp,
    'NORTH (ERCOT)' AS load_zone_name,
    10000712973 AS load_zone_id,
    lfd.load_forecast,
    lfd.load_forecast_publish_date,
    ald."RTLOAD" AS rtload,
    wfd.wind_gen_forecast,
    wfd.wind_gen_forecast_publish_date,
    awg.wind_gen,
    sfd.solar_gen_forecast,
    sfd.solar_gen_forecast_publish_date,
    asg.solar_gen,
    lfd.load_forecast - (wfd.wind_gen_forecast + sfd.solar_gen_forecast) AS net_load_forecast,
    ald."RTLOAD" - (awg.wind_gen + asg.solar_gen) AS net_load_real_time
FROM time_periods tp
LEFT JOIN price_data pd ON tp."DATETIME" = pd."DATETIME"
LEFT JOIN load_forecast_data lfd ON tp."DATETIME" = lfd."DATETIME"
LEFT JOIN wind_forecast_data wfd ON tp."DATETIME" = wfd."DATETIME"
LEFT JOIN solar_forecast_data sfd ON tp."DATETIME" = sfd."DATETIME"
LEFT JOIN actual_load_data ald ON tp."DATETIME" = ald."DATETIME"
LEFT JOIN actual_wind_gen awg ON tp."DATETIME" = awg."DATETIME"
LEFT JOIN actual_solar_gen asg ON tp."DATETIME" = asg."DATETIME"
ORDER BY tp."DATETIME"