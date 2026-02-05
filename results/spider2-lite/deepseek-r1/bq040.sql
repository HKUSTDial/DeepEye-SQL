WITH filtered_trips AS (
  SELECT 
    pickup_location_id,
    tip_amount,
    total_amount,
    dropoff_datetime,
    pickup_datetime,
    passenger_count,
    trip_distance,
    tolls_amount,
    mta_tax,
    fare_amount
  FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2016`
  WHERE pickup_datetime >= '2016-01-01'
    AND pickup_datetime < '2016-01-08'
    AND dropoff_datetime > pickup_datetime
    AND passenger_count > 0
    AND trip_distance >= 0
    AND tip_amount >= 0
    AND tolls_amount >= 0
    AND mta_tax >= 0
    AND fare_amount >= 0
    AND total_amount >= 0
),
joined_trips AS (
  SELECT 
    f.*,
    g.borough
  FROM filtered_trips f
  INNER JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` g
    ON f.pickup_location_id = g.zone_id
  WHERE g.borough NOT IN ('EWR', 'Staten Island')
    AND (f.total_amount - f.tip_amount) > 0
),
trips_with_tip_rate AS (
  SELECT 
    *,
    (tip_amount / (total_amount - tip_amount)) * 100 AS tip_rate
  FROM joined_trips
),
categorized_trips AS (
  SELECT 
    borough,
    CASE 
      WHEN tip_rate = 0 THEN '0% (no tip)'
      WHEN tip_rate <= 5 THEN 'up to 5%'
      WHEN tip_rate <= 10 THEN '5% to 10%'
      WHEN tip_rate <= 15 THEN '10% to 15%'
      WHEN tip_rate <= 20 THEN '15% to 20%'
      WHEN tip_rate <= 25 THEN '20% to 25%'
      ELSE 'More than 25%'
    END AS tip_category
  FROM trips_with_tip_rate
),
counts_per_category AS (
  SELECT 
    borough,
    tip_category,
    COUNT(*) AS trip_count
  FROM categorized_trips
  GROUP BY borough, tip_category
),
proportions AS (
  SELECT 
    borough,
    tip_category,
    trip_count / SUM(trip_count) OVER (PARTITION BY borough) AS proportion
  FROM counts_per_category
)
SELECT 
  borough,
  tip_category,
  proportion
FROM proportions
ORDER BY borough, 
  CASE tip_category
    WHEN '0% (no tip)' THEN 1
    WHEN 'up to 5%' THEN 2
    WHEN '5% to 10%' THEN 3
    WHEN '10% to 15%' THEN 4
    WHEN '15% to 20%' THEN 5
    WHEN '20% to 25%' THEN 6
    WHEN 'More than 25%' THEN 7
  END