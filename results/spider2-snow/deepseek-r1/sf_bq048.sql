WITH wind_speed_raw AS (
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2011"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2012"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2013"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2014"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2015"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2016"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2017"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2018"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    TO_DATE("year" || '-' || "mo" || '-' || "da", 'YYYY-MM-DD') AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2019"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
  UNION ALL
  SELECT 
    "date" AS obs_date,
    TRY_CAST("wdsp" AS FLOAT) AS wdsp_numeric
  FROM "NEW_YORK_NOAA"."NOAA_GSOD"."GSOD2020"
  WHERE "stn" = '744860' AND "wdsp" != '999.9'
),
wind_speed AS (
  SELECT 
    obs_date,
    AVG(wdsp_numeric) AS wdsp_numeric
  FROM wind_speed_raw
  GROUP BY obs_date
),
complaint_data AS (
  SELECT 
    DATE(TO_TIMESTAMP("created_date" / 1000000)) AS complaint_date,
    "complaint_type" AS complaint_type,
    COUNT(*) AS type_count
  FROM "NEW_YORK_NOAA"."NEW_YORK"."_311_SERVICE_REQUESTS"
  WHERE "created_date" IS NOT NULL
    AND YEAR(DATE(TO_TIMESTAMP("created_date" / 1000000))) BETWEEN 2011 AND 2020
  GROUP BY 1, 2
),
daily_totals AS (
  SELECT 
    DATE(TO_TIMESTAMP("created_date" / 1000000)) AS complaint_date,
    COUNT(*) AS total_complaints
  FROM "NEW_YORK_NOAA"."NEW_YORK"."_311_SERVICE_REQUESTS"
  WHERE "created_date" IS NOT NULL
    AND YEAR(DATE(TO_TIMESTAMP("created_date" / 1000000))) BETWEEN 2011 AND 2020
  GROUP BY 1
),
complaint_proportions AS (
  SELECT 
    c.complaint_date,
    c.complaint_type,
    c.type_count,
    t.total_complaints,
    c.type_count / t.total_complaints AS proportion
  FROM complaint_data c
  JOIN daily_totals t ON c.complaint_date = t.complaint_date
),
combined_data AS (
  SELECT 
    cp.complaint_type,
    cp.complaint_date,
    cp.type_count,
    cp.proportion,
    ws.wdsp_numeric
  FROM complaint_proportions cp
  JOIN wind_speed ws ON cp.complaint_date = ws.obs_date
  WHERE ws.wdsp_numeric IS NOT NULL
),
all_correlations AS (
  SELECT 
    complaint_type,
    ROUND(CORR(proportion, wdsp_numeric), 4) AS correlation
  FROM combined_data
  GROUP BY complaint_type
  HAVING SUM(type_count) > 3000
),
ranked AS (
  SELECT 
    complaint_type,
    correlation,
    CASE WHEN correlation > 0 THEN RANK() OVER (ORDER BY correlation DESC) END AS pos_rank,
    CASE WHEN correlation < 0 THEN RANK() OVER (ORDER BY correlation ASC) END AS neg_rank
  FROM all_correlations
)
SELECT complaint_type, correlation
FROM ranked
WHERE (correlation > 0 AND pos_rank = 1)
   OR (correlation < 0 AND neg_rank = 1)
ORDER BY correlation DESC