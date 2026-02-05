WITH trace_deltas AS (
  SELECT "from_address" AS address, -"value" AS delta
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "trace_type" = 'call'
    AND "call_type" NOT IN ('delegatecall', 'callcode', 'staticcall')
    AND "status" = 1
    AND "from_address" IS NOT NULL
    AND "from_address" != '0x0000000000000000000000000000000000000000'
  UNION ALL
  SELECT "to_address" AS address, "value" AS delta
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "trace_type" = 'call'
    AND "call_type" NOT IN ('delegatecall', 'callcode', 'staticcall')
    AND "status" = 1
    AND "to_address" IS NOT NULL
    AND "to_address" != '0x0000000000000000000000000000000000000000'
), block_gas_fees AS (
  SELECT "block_number", SUM("receipt_gas_used" * "gas_price") AS total_gas_fees
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "receipt_gas_used" IS NOT NULL AND "gas_price" IS NOT NULL
  GROUP BY "block_number"
), miner_rewards AS (
  SELECT b."miner" AS address, bgf.total_gas_fees AS delta
  FROM block_gas_fees bgf
  JOIN "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."BLOCKS" b ON bgf."block_number" = b."number"
  WHERE b."miner" IS NOT NULL AND b."miner" != '0x0000000000000000000000000000000000000000'
), sender_deductions AS (
  SELECT "from_address" AS address, -("receipt_gas_used" * "gas_price") AS delta
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "from_address" IS NOT NULL AND "from_address" != '0x0000000000000000000000000000000000000000'
    AND "receipt_gas_used" IS NOT NULL AND "gas_price" IS NOT NULL
), all_deltas AS (
  SELECT * FROM trace_deltas
  UNION ALL
  SELECT * FROM miner_rewards
  UNION ALL
  SELECT * FROM sender_deductions
), net_balances AS (
  SELECT address, SUM(delta) AS net_balance_wei
  FROM all_deltas
  WHERE address IS NOT NULL AND address != '0x0000000000000000000000000000000000000000'
  GROUP BY address
), ranked_balances AS (
  SELECT net_balance_wei, RANK() OVER (ORDER BY net_balance_wei DESC) AS balance_rank
  FROM net_balances
)
SELECT ROUND(AVG(net_balance_wei) / 1000000000000000, 2) AS avg_balance_quadrillions
FROM ranked_balances
WHERE balance_rank <= 10