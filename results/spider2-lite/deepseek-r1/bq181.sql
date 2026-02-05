WITH valid_stations AS (
  SELECT `usaf` FROM `bigquery-public-data.noaa_gsod.stations` WHERE `usaf` != '999999'
),
station_valid_days AS (
  SELECT 
    s.`usaf`,
    COUNT(DISTINCT 
      CASE WHEN g.`temp` IS NOT NULL AND g.`temp` != 9999.9 
           AND g.`max` IS NOT NULL AND g.`max` != 9999.9 
           AND g.`min` IS NOT NULL AND g.`min` != 9999.9 
      THEN g.`date` 
      ELSE NULL 
      END
    ) AS valid_days
  FROM valid_stations s
  LEFT JOIN `bigquery-public-data.noaa_gsod.gsod2022` g ON s.`usaf` = g.`stn`
  GROUP BY s.`usaf`
),
meeting_stations AS (
  SELECT `usaf` FROM station_valid_days WHERE valid_days >= 329
),
total_stations AS (
  SELECT COUNT(*) AS total FROM valid_stations
),
meeting_count AS (
  SELECT COUNT(*) AS meeting FROM meeting_stations
)
SELECT (meeting.meeting * 100.0 / total.total) AS percentage FROM total_stations total, meeting_count meeting