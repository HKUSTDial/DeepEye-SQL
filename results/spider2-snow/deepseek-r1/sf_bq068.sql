WITH eligible_transactions AS (
  SELECT "hash"
  FROM "CRYPTO"."CRYPTO_BITCOIN_CASH"."TRANSACTIONS"
  WHERE TO_TIMESTAMP("block_timestamp" / 1000000) >= '2014-03-01' AND TO_TIMESTAMP("block_timestamp" / 1000000) < '2014-04-01'
), inputs_flattened AS (
  SELECT f.value AS address, i."type", -i."value" AS net_value
  FROM "CRYPTO"."CRYPTO_BITCOIN_CASH"."INPUTS" i
  INNER JOIN eligible_transactions t ON i."transaction_hash" = t."hash"
  CROSS JOIN LATERAL FLATTEN(INPUT => i."addresses") f
), outputs_flattened AS (
  SELECT f.value AS address, o."type", o."value" AS net_value
  FROM "CRYPTO"."CRYPTO_BITCOIN_CASH"."OUTPUTS" o
  INNER JOIN eligible_transactions t ON o."transaction_hash" = t."hash"
  CROSS JOIN LATERAL FLATTEN(INPUT => o."addresses") f
), combined AS (
  SELECT * FROM inputs_flattened
  UNION ALL
  SELECT * FROM outputs_flattened
), address_balances AS (
  SELECT address, ANY_VALUE("type") AS address_type, SUM(net_value) AS final_balance
  FROM combined
  GROUP BY address
)
SELECT address_type, MAX(final_balance) AS max_balance, MIN(final_balance) AS min_balance
FROM address_balances
GROUP BY address_type