WITH transactions_2021 AS (
  SELECT 
    "hash",
    "input_value",
    "output_value",
    "input_count",
    "output_count",
    "block_timestamp_month",
    EXTRACT(MONTH FROM "block_timestamp_month") AS month_num
  FROM "CRYPTO"."CRYPTO_BITCOIN"."TRANSACTIONS"
  WHERE "block_timestamp_month" >= '2021-01-01' AND "block_timestamp_month" <= '2021-12-01'
),
outputs_2021 AS (
  SELECT o."transaction_hash", o."value"
  FROM "CRYPTO"."CRYPTO_BITCOIN"."OUTPUTS" o
  INNER JOIN transactions_2021 t ON o."transaction_hash" = t."hash"
),
duplicate_outputs AS (
  SELECT 
    "transaction_hash",
    CASE WHEN COUNT(*) > COUNT(DISTINCT "value") THEN 1 ELSE 0 END AS has_duplicate_value
  FROM outputs_2021
  GROUP BY "transaction_hash"
),
transactions_with_flag AS (
  SELECT 
    t.*,
    COALESCE(d.has_duplicate_value, 0) AS has_duplicate_value
  FROM transactions_2021 t
  LEFT JOIN duplicate_outputs d ON t."hash" = d."transaction_hash"
),
coinjoin_transactions AS (
  SELECT 
    *,
    CASE WHEN "output_count" > 2 
          AND "output_value" <= "input_value" 
          AND has_duplicate_value = 1 
         THEN 1 ELSE 0 END AS is_coinjoin
  FROM transactions_with_flag
),
monthly_totals AS (
  SELECT 
    month_num,
    COUNT(*) AS total_transactions,
    SUM("output_value") AS total_volume,
    SUM("input_count") AS total_inputs,
    SUM("output_count") AS total_outputs
  FROM coinjoin_transactions
  GROUP BY month_num
),
monthly_coinjoin AS (
  SELECT 
    month_num,
    SUM(is_coinjoin) AS coinjoin_transactions,
    SUM(CASE WHEN is_coinjoin = 1 THEN "output_value" ELSE 0 END) AS coinjoin_volume,
    SUM(CASE WHEN is_coinjoin = 1 THEN "input_count" ELSE 0 END) AS coinjoin_inputs,
    SUM(CASE WHEN is_coinjoin = 1 THEN "output_count" ELSE 0 END) AS coinjoin_outputs
  FROM coinjoin_transactions
  GROUP BY month_num
),
monthly_percentages AS (
  SELECT 
    t.month_num,
    ROUND(100.0 * c.coinjoin_transactions / NULLIF(t.total_transactions, 0), 1) AS pct_transactions,
    ROUND(100.0 * c.coinjoin_inputs / NULLIF(t.total_inputs, 0), 1) AS pct_inputs,
    ROUND(100.0 * c.coinjoin_outputs / NULLIF(t.total_outputs, 0), 1) AS pct_outputs,
    ROUND((pct_inputs + pct_outputs) / 2, 1) AS pct_utxos,
    ROUND(100.0 * c.coinjoin_volume / NULLIF(t.total_volume, 0), 1) AS pct_volume
  FROM monthly_totals t
  JOIN monthly_coinjoin c ON t.month_num = c.month_num
)
SELECT 
  month_num,
  pct_transactions,
  pct_utxos,
  pct_volume
FROM monthly_percentages
ORDER BY pct_volume DESC
LIMIT 1