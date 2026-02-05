WITH calendar AS (
  SELECT 
    DATEADD(day, SEQ4(), '2017-01-01')::DATE as date
  FROM TABLE(GENERATOR(ROWCOUNT => 1827))
  WHERE date <= '2021-12-31'
),
daily_counts AS (
  SELECT 
    date,
    category,
    COUNT(*) as daily_count
  FROM (
    SELECT 
      TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) as date,
      CASE 
        WHEN "trace_address" IS NULL THEN 'external'
        ELSE 'internal'
      END as category
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
    WHERE "trace_type" = 'create'
      AND TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) BETWEEN '2017-01-01' AND '2021-12-31'
    UNION ALL
    SELECT 
      TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) as date,
      CASE 
        WHEN "trace_address" IS NULL THEN 'external'
        ELSE 'internal'
      END as category
    FROM "CRYPTO"."CRYPTO_ETHEREUM_CLASSIC"."TRACES"
    WHERE "trace_type" = 'create'
      AND TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) BETWEEN '2017-01-01' AND '2021-12-31'
  )
  GROUP BY date, category
),
pivoted_counts AS (
  SELECT 
    c.date,
    COALESCE(SUM(CASE WHEN dc.category = 'external' THEN dc.daily_count END), 0) as external_daily,
    COALESCE(SUM(CASE WHEN dc.category = 'internal' THEN dc.daily_count END), 0) as internal_daily
  FROM calendar c
  LEFT JOIN daily_counts dc ON c.date = dc.date
  GROUP BY c.date
)
SELECT 
  date,
  SUM(external_daily) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_external,
  SUM(internal_daily) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_internal
FROM pivoted_counts
ORDER BY date