WITH filtered_transactions AS (
  SELECT 
    "hash",
    "output_count",
    "output_value",
    "input_value",
    "input_count",
    "block_timestamp_month"
  FROM "CRYPTO"."CRYPTO_BITCOIN"."TRANSACTIONS"
  WHERE "block_timestamp_month" >= '2023-07-01'
),
transactions_with_identical_outputs AS (
  SELECT 
    o."transaction_hash",
    COUNT(DISTINCT o."value") as distinct_output_values,
    COUNT(*) as total_outputs
  FROM "CRYPTO"."CRYPTO_BITCOIN"."OUTPUTS" o
  INNER JOIN filtered_transactions ft ON o."transaction_hash" = ft."hash"
  GROUP BY o."transaction_hash"
  HAVING COUNT(DISTINCT o."value") < COUNT(*) 
),
coinjoin_transactions AS (
  SELECT 
    ft."hash",
    ft."output_count",
    ft."output_value",
    ft."input_value", 
    ft."input_count",
    ft."block_timestamp_month"
  FROM filtered_transactions ft
  INNER JOIN transactions_with_identical_outputs tio ON ft."hash" = tio."transaction_hash"
  WHERE ft."output_count" > 2
    AND ft."output_value" <= ft."input_value"
),
monthly_totals AS (
  SELECT 
    ft."block_timestamp_month" as month,
    COUNT(DISTINCT ft."hash") as total_transactions,
    COUNT(DISTINCT CASE WHEN ct."hash" IS NOT NULL THEN ft."hash" END) as coinjoin_transactions,
    SUM(ft."input_count" + ft."output_count") as total_utxos,
    SUM(CASE WHEN ct."hash" IS NOT NULL THEN ft."input_count" + ft."output_count" ELSE 0 END) as coinjoin_utxos,
    SUM(ft."input_value") as total_volume,
    SUM(CASE WHEN ct."hash" IS NOT NULL THEN ft."input_value" ELSE 0 END) as coinjoin_volume
  FROM filtered_transactions ft
  LEFT JOIN coinjoin_transactions ct ON ft."hash" = ct."hash"
  GROUP BY ft."block_timestamp_month"
)
SELECT 
  month,
  (coinjoin_transactions * 100.0 / NULLIF(total_transactions, 0)) as transaction_percentage,
  (coinjoin_utxos * 100.0 / NULLIF(total_utxos, 0)) as utxo_percentage,
  (coinjoin_volume * 100.0 / NULLIF(total_volume, 0)) as volume_percentage
FROM monthly_totals
ORDER BY month