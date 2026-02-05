WITH inputs_debits AS (
  SELECT 
    f.value AS address,
    i."type" AS type,
    -i."value" AS signed_value
  FROM "CRYPTO"."CRYPTO_BITCOIN_CASH"."INPUTS" i,
  LATERAL FLATTEN(input => i."addresses") f
  WHERE TO_TIMESTAMP(i."block_timestamp", 6) >= '2014-03-01'::TIMESTAMP
    AND TO_TIMESTAMP(i."block_timestamp", 6) < '2014-04-01'::TIMESTAMP
),
outputs_credits AS (
  SELECT 
    f.value AS address,
    o."type" AS type,
    o."value" AS signed_value
  FROM "CRYPTO"."CRYPTO_BITCOIN_CASH"."OUTPUTS" o,
  LATERAL FLATTEN(input => o."addresses") f
  WHERE TO_TIMESTAMP(o."block_timestamp", 6) >= '2014-03-01'::TIMESTAMP
    AND TO_TIMESTAMP(o."block_timestamp", 6) < '2014-04-01'::TIMESTAMP
),
combined AS (
  SELECT address, type, signed_value FROM inputs_debits
  UNION ALL
  SELECT address, type, signed_value FROM outputs_credits
),
address_balances AS (
  SELECT address, type, SUM(signed_value) AS final_balance
  FROM combined
  GROUP BY address, type
)
SELECT type, MAX(final_balance) AS max_final_balance, MIN(final_balance) AS min_final_balance
FROM address_balances
GROUP BY type