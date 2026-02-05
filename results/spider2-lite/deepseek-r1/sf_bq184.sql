WITH all_creations AS (
  SELECT
    TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) as creation_date,
    CASE WHEN "trace_address" IS NULL THEN 'external' ELSE 'contract' END as category
  FROM "CRYPTO"."CRYPTO_ETHEREUM_CLASSIC"."TRACES"
  WHERE "trace_type" = 'create'
    AND TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) BETWEEN '2017-01-01' AND '2021-12-31'
  UNION ALL
  SELECT
    TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) as creation_date,
    CASE WHEN "trace_address" IS NULL THEN 'external' ELSE 'contract' END as category
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
  WHERE "trace_type" = 'create'
    AND TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) BETWEEN '2017-01-01' AND '2021-12-31'
),
date_series AS (
  SELECT DATEADD(day, SEQ4(), '2017-01-01')::DATE as date
  FROM TABLE(GENERATOR(ROWCOUNT => 1827))
  WHERE date <= '2021-12-31'
),
daily_counts AS (
  SELECT
    ds.date,
    COUNT_IF(ac.category = 'external') as external_new,
    COUNT_IF(ac.category = 'contract') as contract_new
  FROM date_series ds
  LEFT JOIN all_creations ac ON ds.date = ac.creation_date
  GROUP BY ds.date
)
SELECT
  date,
  SUM(external_new) OVER (ORDER BY date) as cumulative_external,
  SUM(contract_new) OVER (ORDER BY date) as cumulative_contract
FROM daily_counts
ORDER BY date