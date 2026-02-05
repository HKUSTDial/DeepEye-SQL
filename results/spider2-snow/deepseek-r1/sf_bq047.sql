WITH temperature_data AS (
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2008"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2009"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2010"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2011"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2012"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2013"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2014"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2015"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2016"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS "date",
    "stn",
    "temp"
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2017"
  WHERE "stn" IN ('725030', '744860') AND "temp" != 9999.9
    AND "year" || '-' || "mo" || '-' || "da" BETWEEN '2008-01-01' AND '2017-12-31'
),
station_temps AS (
  SELECT 
    "date",
    MAX(CASE WHEN "stn" = '725030' THEN "temp" END) AS temp_lga,
    MAX(CASE WHEN "stn" = '744860' THEN "temp" END) AS temp_jfk
  FROM temperature_data
  GROUP BY "date"
  HAVING temp_lga IS NOT NULL AND temp_jfk IS NOT NULL
),
avg_temps AS (
  SELECT 
    "date",
    (temp_lga + temp_jfk) / 2 AS avg_temp
  FROM station_temps
),
valid_days_count AS (
  SELECT COUNT(*) AS total_valid_days FROM avg_temps
),
complaints_data AS (
  SELECT 
    DATE(TO_TIMESTAMP("created_date" / 1000000)) AS "date",
    "complaint_type",
    COUNT(*) AS daily_count
  FROM "NEW_YORK_NOAA"."NEW_YORK"."_311_SERVICE_REQUESTS"
  WHERE "created_date" IS NOT NULL
  GROUP BY "date", "complaint_type"
),
total_daily_complaints AS (
  SELECT 
    "date",
    SUM(daily_count) AS daily_total
  FROM complaints_data
  GROUP BY "date"
),
complaint_totals AS (
  SELECT 
    "complaint_type",
    SUM(daily_count) AS total_complaints
  FROM complaints_data
  GROUP BY "complaint_type"
  HAVING total_complaints > 5000
),
all_combos AS (
  SELECT 
    a."date",
    c."complaint_type"
  FROM avg_temps a
  CROSS JOIN complaint_totals c
),
combined_data AS (
  SELECT 
    ac."date",
    ac."complaint_type",
    at.avg_temp,
    COALESCE(cd.daily_count, 0) AS daily_count,
    COALESCE(tdc.daily_total, 0) AS daily_total
  FROM all_combos ac
  JOIN avg_temps at ON ac."date" = at."date"
  LEFT JOIN complaints_data cd ON ac."date" = cd."date" AND ac."complaint_type" = cd."complaint_type"
  LEFT JOIN total_daily_complaints tdc ON ac."date" = tdc."date"
  WHERE at."date" BETWEEN '2008-01-01' AND '2017-12-31'
    AND ac."date" BETWEEN '2008-01-01' AND '2017-12-31'
),
correlation_data AS (
  SELECT 
    "complaint_type",
    CORR(avg_temp, daily_count) AS corr_count,
    CORR(avg_temp, CASE WHEN daily_total > 0 THEN daily_count * 1.0 / daily_total ELSE NULL END) AS corr_pct
  FROM combined_data
  GROUP BY "complaint_type"
)
SELECT 
  cd."complaint_type",
  ct.total_complaints,
  vdc.total_valid_days,
  ROUND(cd.corr_count, 4) AS correlation_with_daily_count,
  ROUND(cd.corr_pct, 4) AS correlation_with_daily_percentage
FROM correlation_data cd
JOIN complaint_totals ct ON cd."complaint_type" = ct."complaint_type"
CROSS JOIN valid_days_count vdc
WHERE (ABS(cd.corr_count) > 0.5 OR ABS(cd.corr_pct) > 0.5)
ORDER BY cd."complaint_type"