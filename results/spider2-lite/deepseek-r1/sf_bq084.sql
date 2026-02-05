WITH all_transactions_2023 AS (
  SELECT TO_TIMESTAMP("block_timestamp") AS block_timestamp FROM "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_CRONOS_MAINNET_US"."TRANSACTIONS" WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("block_timestamp")) = 2023
  UNION ALL
  SELECT TO_TIMESTAMP("block_timestamp") AS block_timestamp FROM "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_POLYGON_MAINNET_US"."TRANSACTIONS" WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("block_timestamp")) = 2023
),
monthly_counts AS (
  SELECT 
    EXTRACT(YEAR FROM block_timestamp) AS year,
    EXTRACT(MONTH FROM block_timestamp) AS month,
    COUNT(*) AS transaction_count
  FROM all_transactions_2023
  GROUP BY year, month
)
SELECT 
  transaction_count,
  transaction_count::NUMERIC / (DAY(LAST_DAY(DATE_FROM_PARTS(year, month, 1))) * 24 * 60 * 60) AS transactions_per_second,
  year,
  month
FROM monthly_counts
ORDER BY transaction_count DESC