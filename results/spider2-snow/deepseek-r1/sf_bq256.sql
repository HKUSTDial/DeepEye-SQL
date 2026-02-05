WITH target_address AS (
  SELECT "from_address" AS address
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
  WHERE "block_timestamp" < 1630454400000000 AND "receipt_status" = 1
  GROUP BY "from_address"
  ORDER BY COUNT(*) DESC
  LIMIT 1
), tx_in AS (
  SELECT COALESCE(SUM("value"), 0) AS sum_in
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
  WHERE "to_address" = (SELECT address FROM target_address)
    AND "block_timestamp" < 1630454400000000
    AND "receipt_status" = 1
), tx_out AS (
  SELECT COALESCE(SUM("value"), 0) AS sum_out
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
  WHERE "from_address" = (SELECT address FROM target_address)
    AND "block_timestamp" < 1630454400000000
    AND "receipt_status" = 1
), gas_fees AS (
  SELECT COALESCE(SUM("receipt_gas_used" * "receipt_effective_gas_price"), 0) AS gas_total
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
  WHERE "from_address" = (SELECT address FROM target_address)
    AND "block_timestamp" < 1630454400000000
), trace_in AS (
  SELECT COALESCE(SUM("value"), 0) AS sum_in
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
  WHERE "to_address" = (SELECT address FROM target_address)
    AND "block_timestamp" < 1630454400000000
    AND "status" = 1
    AND ("trace_type" != 'call' OR "call_type" NOT IN ('delegatecall', 'callcode', 'staticcall'))
), trace_out AS (
  SELECT COALESCE(SUM("value"), 0) AS sum_out
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
  WHERE "from_address" = (SELECT address FROM target_address)
    AND "block_timestamp" < 1630454400000000
    AND "status" = 1
    AND ("trace_type" != 'call' OR "call_type" NOT IN ('delegatecall', 'callcode', 'staticcall'))
), rewards AS (
  SELECT COALESCE(SUM("value"), 0) AS sum_rewards
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
  WHERE "to_address" = (SELECT address FROM target_address)
    AND "block_timestamp" < 1630454400000000
    AND "trace_type" = 'reward'
)
SELECT ((tx_in.sum_in + trace_in.sum_in + rewards.sum_rewards) - (tx_out.sum_out + trace_out.sum_out + gas_fees.gas_total)) / POWER(10,18) AS final_balance_ether
FROM tx_in, tx_out, gas_fees, trace_in, trace_out, rewards