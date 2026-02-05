WITH april_transactions AS (
  SELECT "hash"
  FROM "CRYPTO"."CRYPTO_DASH"."TRANSACTIONS"
  WHERE "block_timestamp_month" = DATE '2023-04-01'
),
input_data AS (
  SELECT
    ARRAY_TO_STRING(PARSE_JSON(i."addresses")::ARRAY, ',') AS address,
    i."type",
    -i."value" AS amount
  FROM "CRYPTO"."CRYPTO_DASH"."INPUTS" i
  INNER JOIN april_transactions t ON i."transaction_hash" = t."hash"
  WHERE i."addresses" IS NOT NULL
),
output_data AS (
  SELECT
    ARRAY_TO_STRING(PARSE_JSON(o."addresses")::ARRAY, ',') AS address,
    o."type",
    o."value" AS amount
  FROM "CRYPTO"."CRYPTO_DASH"."OUTPUTS" o
  INNER JOIN april_transactions t ON o."transaction_hash" = t."hash"
  WHERE o."addresses" IS NOT NULL
),
all_transfers AS (
  SELECT address, "type", amount FROM input_data
  UNION ALL
  SELECT address, "type", amount FROM output_data
),
balances AS (
  SELECT
    address,
    "type",
    SUM(amount) AS balance
  FROM all_transfers
  GROUP BY address, "type"
)
SELECT
  MAX(balance) AS highest_balance,
  MIN(balance) AS lowest_balance
FROM balances