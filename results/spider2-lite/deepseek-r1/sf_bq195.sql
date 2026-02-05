WITH balance_changes AS (
  -- From Ethereum transactions (direct ETH transfers and gas fees)
  SELECT "from_address" AS "address", -("value" + COALESCE("receipt_effective_gas_price", "gas_price") * "receipt_gas_used") AS "change"
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
  WHERE "receipt_status" = 1
    AND "block_timestamp" < 1630454400000000
    AND "from_address" IS NOT NULL
    
  UNION ALL
  
  SELECT "to_address" AS "address", "value" AS "change"
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
  WHERE "receipt_status" = 1
    AND "block_timestamp" < 1630454400000000
    AND "to_address" IS NOT NULL
    
  UNION ALL
  
  -- From Ethereum traces (internal calls)
  SELECT "from_address" AS "address", -"value" AS "change"
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
  WHERE "status" = 1
    AND ("call_type" IS NULL OR "call_type" = 'call')
    AND "block_timestamp" < 1630454400000000
    AND "from_address" IS NOT NULL
    
  UNION ALL
  
  SELECT "to_address" AS "address", "value" AS "change"
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
  WHERE "status" = 1
    AND ("call_type" IS NULL OR "call_type" = 'call')
    AND "block_timestamp" < 1630454400000000
    AND "to_address" IS NOT NULL
)
SELECT "address", SUM("change") AS "balance"
FROM balance_changes
GROUP BY "address"
ORDER BY "balance" DESC
LIMIT 10