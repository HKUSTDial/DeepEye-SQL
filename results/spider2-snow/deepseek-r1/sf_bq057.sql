WITH transactions_2021 AS (
  SELECT 
    "hash",
    "output_count",
    "input_count",
    "output_value",
    "input_value",
    "block_timestamp_month",
    EXTRACT(MONTH FROM "block_timestamp_month") AS "month"
  FROM "CRYPTO"."CRYPTO_BITCOIN"."TRANSACTIONS"
  WHERE "block_timestamp_month" >= '2021-01-01' AND "block_timestamp_month" <= '2021-12-01'
),
multiple_equal_outputs AS (
  SELECT 
    "transaction_hash"
  FROM "CRYPTO"."CRYPTO_BITCOIN"."OUTPUTS"
  WHERE "transaction_hash" IN (SELECT "hash" FROM transactions_2021)
  GROUP BY "transaction_hash", "value"
  HAVING COUNT(*) >= 2
),
transactions_with_flag AS (
  SELECT 
    t.*,
    CASE 
      WHEN t."output_count" > 2 
           AND t."output_value" <= t."input_value"
           AND m."transaction_hash" IS NOT NULL THEN 1
      ELSE 0
    END AS "is_coinjoin"
  FROM transactions_2021 t
  LEFT JOIN multiple_equal_outputs m ON t."hash" = m."transaction_hash"
),
monthly_aggregates AS (
  SELECT 
    "month",
    COUNT(*) AS "total_transactions",
    SUM("is_coinjoin") AS "coinjoin_transactions",
    SUM("output_value") AS "total_volume",
    SUM(CASE WHEN "is_coinjoin" = 1 THEN "output_value" ELSE 0 END) AS "coinjoin_volume",
    SUM("input_count") AS "total_inputs",
    SUM(CASE WHEN "is_coinjoin" = 1 THEN "input_count" ELSE 0 END) AS "coinjoin_inputs",
    SUM("output_count") AS "total_outputs",
    SUM(CASE WHEN "is_coinjoin" = 1 THEN "output_count" ELSE 0 END) AS "coinjoin_outputs"
  FROM transactions_with_flag
  GROUP BY "month"
),
monthly_percentages AS (
  SELECT 
    "month",
    ROUND(100.0 * "coinjoin_transactions" / NULLIF("total_transactions",0), 1) AS "pct_transactions",
    ROUND(100.0 * "coinjoin_volume" / NULLIF("total_volume",0), 1) AS "pct_volume",
    ROUND(100.0 * "coinjoin_inputs" / NULLIF("total_inputs",0), 1) AS "pct_inputs",
    ROUND(100.0 * "coinjoin_outputs" / NULLIF("total_outputs",0), 1) AS "pct_outputs"
  FROM monthly_aggregates
),
monthly_utxo_pct AS (
  SELECT 
    "month",
    "pct_transactions",
    "pct_volume",
    ROUND(("pct_inputs" + "pct_outputs") / 2, 1) AS "pct_utxos"
  FROM monthly_percentages
)
SELECT 
  "month",
  "pct_transactions",
  "pct_utxos",
  "pct_volume"
FROM monthly_utxo_pct
ORDER BY "pct_volume" DESC
LIMIT 1