WITH filtered_transactions AS (
  SELECT
    "block_timestamp",
    "value"
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TOKEN_TRANSFERS"
  WHERE "token_address" = '0x68e54af74b22acaccffa04ccaad13be16ed14eac'
    AND ("from_address" = '0x8babf0ba311aab914c00e8fda7e8558a8b66de5d' OR "to_address" = '0xfbd6c6b112214d949dcdfb1217153bc0a742862f')
    AND "block_timestamp" >= 1546300800000000
    AND "block_timestamp" <= 1609459199000000
),
hourly_totals AS (
  SELECT
    DATE_TRUNC('hour', TO_TIMESTAMP("block_timestamp"::NUMERIC / 1000000)) AS hour,
    SUM(TRY_CAST("value" AS NUMERIC)) AS total_value
  FROM filtered_transactions
  GROUP BY DATE_TRUNC('hour', TO_TIMESTAMP("block_timestamp"::NUMERIC / 1000000))
),
hourly_changes AS (
  SELECT
    hour,
    total_value,
    total_value - LAG(total_value) OVER (ORDER BY hour) AS change
  FROM hourly_totals
),
yearly_avg_changes AS (
  SELECT
    EXTRACT(YEAR FROM hour) AS year,
    AVG(change) AS avg_hourly_change
  FROM hourly_changes
  WHERE change IS NOT NULL
  GROUP BY EXTRACT(YEAR FROM hour)
)
SELECT
  MAX(CASE WHEN year = 2020 THEN avg_hourly_change END) -
  MAX(CASE WHEN year = 2019 THEN avg_hourly_change END) AS difference
FROM yearly_avg_changes