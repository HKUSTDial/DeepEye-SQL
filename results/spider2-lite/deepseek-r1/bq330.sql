WITH colorado_zips AS (
  SELECT zip_code, zip_code_geom
  FROM `bigquery-public-data.geo_us_boundaries.zip_codes`
  WHERE state_name = 'Colorado'
),
colorado_blockgroups AS (
  SELECT geo_id, blockgroup_geom, ST_AREA(blockgroup_geom) AS blockgroup_area
  FROM `bigquery-public-data.geo_census_blockgroups.us_blockgroups_national`
  WHERE state_name = 'Colorado'
),
bank_counts AS (
  SELECT zip_code, COUNT(*) AS bank_count
  FROM `bigquery-public-data.fdic_banks.locations`
  WHERE state = 'CO' OR state_name = 'Colorado'
  GROUP BY zip_code
),
overlaps AS (
  SELECT 
    z.zip_code,
    b.geo_id,
    ST_AREA(ST_INTERSECTION(z.zip_code_geom, b.blockgroup_geom)) AS overlap_area,
    b.blockgroup_area
  FROM colorado_zips z
  JOIN colorado_blockgroups b
  ON ST_INTERSECTS(z.zip_code_geom, b.blockgroup_geom)
  WHERE b.blockgroup_area > 0
),
allocated_banks AS (
  SELECT 
    o.zip_code,
    o.geo_id,
    bc.bank_count * (o.overlap_area / o.blockgroup_area) AS allocated_bank_count
  FROM overlaps o
  JOIN bank_counts bc ON o.zip_code = bc.zip_code
),
zip_concentration AS (
  SELECT 
    zip_code,
    MAX(allocated_bank_count) AS max_banks_per_blockgroup
  FROM allocated_banks
  GROUP BY zip_code
)
SELECT zip_code
FROM zip_concentration
ORDER BY max_banks_per_blockgroup DESC
LIMIT 1