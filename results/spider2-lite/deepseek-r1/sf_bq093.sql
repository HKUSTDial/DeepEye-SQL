WITH transaction_data AS (
  SELECT
    t."from_address",
    t."to_address",
    t."value",
    t."receipt_gas_used",
    t."gas_price",
    b."miner",
    (t."receipt_gas_used" * t."gas_price") AS gas_fee
  FROM "CRYPTO"."CRYPTO_ETHEREUM_CLASSIC"."TRANSACTIONS" t
  INNER JOIN "CRYPTO"."CRYPTO_ETHEREUM_CLASSIC"."BLOCKS" b
    ON t."block_number" = b."number"
  WHERE DATE(TO_TIMESTAMP(t."block_timestamp" / 1000000)) = DATE '2016-10-14'
    AND t."receipt_status" = 1
),
balance_changes AS (
  SELECT
    "from_address" AS address,
    - ("value" + gas_fee) AS change
  FROM transaction_data
  WHERE "from_address" IS NOT NULL
  UNION ALL
  SELECT
    "to_address" AS address,
    "value" AS change
  FROM transaction_data
  WHERE "to_address" IS NOT NULL
  UNION ALL
  SELECT
    "miner" AS address,
    gas_fee AS change
  FROM transaction_data
  WHERE "miner" IS NOT NULL
),
net_changes AS (
  SELECT
    address,
    SUM(change) AS net_balance_change
  FROM balance_changes
  GROUP BY address
)
SELECT
  MAX(net_balance_change) AS max_net_balance_change,
  MIN(net_balance_change) AS min_net_balance_change
FROM net_changes