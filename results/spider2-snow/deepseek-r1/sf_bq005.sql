WITH blocks_2023 AS (
  SELECT
    "number",
    "timestamp"
  FROM
    "CRYPTO"."CRYPTO_BITCOIN"."BLOCKS"
  WHERE
    "timestamp" >= 1672531200000000
    AND "timestamp" < 1704067200000000
    AND "number" != 0
),
numbered_blocks AS (
  SELECT
    "number",
    "timestamp",
    ROW_NUMBER() OVER (ORDER BY "number") AS rn
  FROM
    blocks_2023
),
consecutive_pairs AS (
  SELECT
    curr."timestamp" AS curr_ts,
    next."timestamp" AS next_ts,
    (next."timestamp" - curr."timestamp") / 1000000 AS interval_seconds
  FROM
    numbered_blocks curr
    INNER JOIN numbered_blocks next ON curr.rn = next.rn - 1
),
daily_avg AS (
  SELECT
    DATE(TO_TIMESTAMP(curr_ts / 1000000)) AS block_date,
    AVG(interval_seconds) AS avg_interval_seconds
  FROM
    consecutive_pairs
  GROUP BY
    block_date
)
SELECT
  block_date,
  avg_interval_seconds
FROM
  daily_avg
ORDER BY
  block_date
LIMIT 10