WITH "trace_balances" AS (
  SELECT "address", SUM("amount") AS "net_amount"
  FROM (
    SELECT "tr"."from_address" AS "address", -"tr"."value" AS "amount"
    FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" AS "tr"
    JOIN "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" AS "tx" ON "tr"."transaction_hash" = "tx"."hash"
    WHERE "tr"."status" = 1
      AND "tr"."trace_type" = 'call'
      AND "tr"."call_type" NOT IN ('delegatecall', 'callcode', 'staticcall')
      AND "tr"."from_address" IS NOT NULL
      AND "tx"."receipt_status" = 1
    UNION ALL
    SELECT "tr"."to_address" AS "address", "tr"."value" AS "amount"
    FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" AS "tr"
    JOIN "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" AS "tx" ON "tr"."transaction_hash" = "tx"."hash"
    WHERE "tr"."status" = 1
      AND "tr"."trace_type" = 'call'
      AND "tr"."call_type" NOT IN ('delegatecall', 'callcode', 'staticcall')
      AND "tr"."to_address" IS NOT NULL
      AND "tx"."receipt_status" = 1
  )
  GROUP BY "address"
),
"miner_fees" AS (
  SELECT "b"."miner" AS "address", SUM("t"."receipt_gas_used" * "t"."gas_price") AS "amount"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."BLOCKS" AS "b"
  JOIN "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" AS "t" ON "b"."number" = "t"."block_number"
  WHERE "b"."miner" IS NOT NULL
  GROUP BY "b"."miner"
),
"sender_gas" AS (
  SELECT "from_address" AS "address", -SUM("receipt_gas_used" * "gas_price") AS "amount"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "from_address" IS NOT NULL
  GROUP BY "from_address"
),
"all_balances" AS (
  SELECT "address", "net_amount" FROM "trace_balances"
  UNION ALL
  SELECT "address", "amount" FROM "miner_fees"
  UNION ALL
  SELECT "address", "amount" FROM "sender_gas"
),
"aggregated_balances" AS (
  SELECT "address", SUM("net_amount") AS "net_balance"
  FROM "all_balances"
  GROUP BY "address"
),
"top_10" AS (
  SELECT "address", "net_balance"
  FROM "aggregated_balances"
  ORDER BY "net_balance" DESC
  LIMIT 10
)
SELECT ROUND(AVG("net_balance") / 1000000000000000, 2) AS "average_balance_quadrillions"
FROM "top_10"