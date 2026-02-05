WITH station_periods AS (
  SELECT 
    usaf,
    wban,
    PARSE_DATE('%Y%m%d', `begin`) AS begin_date,
    PARSE_DATE('%Y%m%d', `end`) AS end_date
  FROM `bigquery-public-data.noaa_gsod.stations`
  WHERE `begin` <= '20000101' AND `end` >= '20190630'
),
station_2019_coverage AS (
  SELECT
    usaf,
    wban,
    GREATEST(DATE '2019-01-01', begin_date) AS start_2019,
    LEAST(DATE '2019-12-31', end_date) AS end_2019
  FROM station_periods
  WHERE begin_date <= DATE '2019-12-31' AND end_date >= DATE '2019-01-01'
),
valid_days_per_station AS (
  SELECT
    s.usaf,
    s.wban,
    s.start_2019,
    s.end_2019,
    COUNT(DISTINCT PARSE_DATE('%Y%m%d', CONCAT(g.`year`, g.`mo`, g.`da`))) AS valid_days_count
  FROM station_2019_coverage s
  INNER JOIN `bigquery-public-data.noaa_gsod.gsod2019` g
    ON s.usaf = g.`stn` AND s.wban = g.`wban`
    AND PARSE_DATE('%Y%m%d', CONCAT(g.`year`, g.`mo`, g.`da`)) BETWEEN s.start_2019 AND s.end_2019
  WHERE 
    g.`temp` != 9999.9 
    AND g.`max` != 9999.9 
    AND g.`min` != 9999.9
  GROUP BY s.usaf, s.wban, s.start_2019, s.end_2019
),
station_stats AS (
  SELECT
    usaf,
    wban,
    valid_days_count,
    DATE_DIFF(end_2019, start_2019, DAY) + 1 AS total_possible_days
  FROM valid_days_per_station
)
SELECT COUNT(*) AS station_count
FROM station_stats
WHERE valid_days_count >= 0.9 * total_possible_days