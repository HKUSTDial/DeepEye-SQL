WITH cutoff_timestamp AS (
  SELECT 1483228800000000 AS cutoff
),
active_addresses AS (
  SELECT DISTINCT "address"
  FROM (
    SELECT "from_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    UNION ALL
    SELECT "to_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp) AND "to_address" IS NOT NULL
    UNION ALL
    SELECT "from_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    UNION ALL
    SELECT "to_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp) AND "to_address" IS NOT NULL
    UNION ALL
    SELECT "from_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS" WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    UNION ALL
    SELECT "to_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS" WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    UNION ALL
    SELECT "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."CONTRACTS" WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
  )
),
balance_inflows AS (
  SELECT
    "to_address" AS "address",
    SUM("value") AS "inflow"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "status" = 1
    AND ("call_type" NOT IN ('delegatecall', 'callcode', 'staticcall') OR "call_type" IS NULL)
    AND "value" > 0
  GROUP BY "to_address"
  UNION ALL
  SELECT
    "to_address" AS "address",
    SUM("value") AS "inflow"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "receipt_status" = 1
    AND "value" > 0
  GROUP BY "to_address"
),
balance_outflows AS (
  SELECT
    "from_address" AS "address",
    SUM("value") AS "outflow"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "status" = 1
    AND ("call_type" NOT IN ('delegatecall', 'callcode', 'staticcall') OR "call_type" IS NULL)
    AND "value" > 0
  GROUP BY "from_address"
  UNION ALL
  SELECT
    "from_address" AS "address",
    SUM("value") AS "outflow"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "receipt_status" = 1
    AND "value" > 0
  GROUP BY "from_address"
),
transaction_fees AS (
  SELECT
    "from_address" AS "address",
    SUM("gas_price" * "receipt_gas_used") AS "fees"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
  GROUP BY "from_address"
),
balance_summary AS (
  SELECT
    a."address",
    COALESCE(i."inflow", 0) - COALESCE(o."outflow", 0) - COALESCE(f."fees", 0) AS "balance_wei"
  FROM active_addresses a
  LEFT JOIN balance_inflows i ON a."address" = i."address"
  LEFT JOIN balance_outflows o ON a."address" = o."address"
  LEFT JOIN transaction_fees f ON a."address" = f."address"
),
activity_data AS (
  SELECT
    "from_address" AS "address",
    TO_TIMESTAMP("block_timestamp" / 1000000) AS "ts"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND ("call_type" NOT IN ('delegatecall', 'callcode', 'staticcall') OR "call_type" IS NULL)
  UNION ALL
  SELECT
    "to_address" AS "address",
    TO_TIMESTAMP("block_timestamp" / 1000000) AS "ts"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND ("call_type" NOT IN ('delegatecall', 'callcode', 'staticcall') OR "call_type" IS NULL)
    AND "to_address" IS NOT NULL
  UNION ALL
  SELECT
    "from_address" AS "address",
    TO_TIMESTAMP("block_timestamp" / 1000000) AS "ts"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
  UNION ALL
  SELECT
    "to_address" AS "address",
    TO_TIMESTAMP("block_timestamp" / 1000000) AS "ts"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "to_address" IS NOT NULL
),
activity_pattern AS (
  SELECT
    "address",
    COUNT(*) AS "activity_count",
    COUNT(DISTINCT DATE("ts")) AS "active_days",
    CASE
      WHEN COUNT(*) > 24 THEN SQRT(POW(SUM(COS(2 * PI() * EXTRACT(HOUR FROM "ts") / 24)), 2) + POW(SUM(SIN(2 * PI() * EXTRACT(HOUR FROM "ts") / 24)), 2)) / COUNT(*)
      ELSE NULL
    END AS "R_active_hour"
  FROM activity_data
  GROUP BY "address"
),
incoming_traces AS (
  SELECT
    "to_address" AS "address",
    COUNT(*) AS "in_trace_count",
    COUNT(DISTINCT "from_address") AS "in_addr_count",
    SUM(CASE WHEN "value" > 0 THEN 1 ELSE 0 END) AS "in_transfer_count",
    AVG(CASE WHEN "value" > 0 THEN "value" / 1e18 END) AS "in_avg_amount",
    AVG(CASE WHEN "trace_type" = 'call' AND "call_type" = 'call' THEN "gas_used" END) AS "avg_gas_used",
    STDDEV(CASE WHEN "trace_type" = 'call' AND "call_type" = 'call' THEN "gas_used" END) AS "std_gas_used"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "status" = 1
    AND ("call_type" NOT IN ('delegatecall', 'callcode', 'staticcall') OR "call_type" IS NULL)
  GROUP BY "to_address"
),
outgoing_traces AS (
  SELECT
    "from_address" AS "address",
    COUNT(*) AS "out_trace_count",
    COUNT(DISTINCT "to_address") AS "out_addr_count",
    SUM(CASE WHEN "value" > 0 THEN 1 ELSE 0 END) AS "out_transfer_count",
    AVG(CASE WHEN "value" > 0 THEN "value" / 1e18 END) AS "out_avg_amount"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "status" = 1
    AND ("call_type" NOT IN ('delegatecall', 'callcode', 'staticcall') OR "call_type" IS NULL)
  GROUP BY "from_address"
),
token_in AS (
  SELECT
    "to_address" AS "address",
    COUNT(*) AS "token_in_tnx",
    COUNT(DISTINCT "token_address") AS "token_in_type",
    COUNT(DISTINCT "from_address") AS "token_from_addr"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
  GROUP BY "to_address"
),
token_out AS (
  SELECT
    "from_address" AS "address",
    COUNT(*) AS "token_out_tnx",
    COUNT(DISTINCT "token_address") AS "token_out_type",
    COUNT(DISTINCT "to_address") AS "token_to_addr"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
  GROUP BY "from_address"
),
mining_rewards AS (
  SELECT
    "to_address" AS "address",
    SUM("value") / 1e18 AS "reward_amount"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "trace_type" = 'reward'
    AND "reward_type" = 'block'
  GROUP BY "to_address"
),
contract_creation AS (
  SELECT
    "from_address" AS "address",
    COUNT(DISTINCT "to_address") AS "contract_create_count"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "trace_type" = 'create'
  GROUP BY "from_address"
),
failure_counts AS (
  SELECT
    "from_address" AS "address",
    COUNT(*) AS "failure_count"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "status" = 0
  GROUP BY "from_address"
  UNION ALL
  SELECT
    "from_address" AS "address",
    COUNT(*) AS "failure_count"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND "receipt_status" = 0
  GROUP BY "from_address"
),
failure_summary AS (
  SELECT
    "address",
    SUM("failure_count") AS "failure_count"
  FROM failure_counts
  GROUP BY "address"
),
bytecode_sizes AS (
  SELECT
    t."from_address" AS "address",
    SUM(LENGTH(c."bytecode")) AS "bytecode_size"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" t
  JOIN "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."CONTRACTS" c ON t."to_address" = c."address"
  WHERE t."block_timestamp" < (SELECT cutoff FROM cutoff_timestamp)
    AND t."trace_type" = 'create'
  GROUP BY t."from_address"
)
SELECT
  a."address",
  b."balance_wei" / 1e18 AS "balance",
  ap."R_active_hour",
  ap."active_days",
  COALESCE(it."in_trace_count", 0) AS "in_trace_count",
  COALESCE(it."in_addr_count", 0) AS "in_addr_count",
  COALESCE(it."in_transfer_count", 0) AS "in_transfer_count",
  COALESCE(it."in_avg_amount", 0) AS "in_avg_amount",
  COALESCE(it."avg_gas_used", 0) AS "avg_gas_used",
  COALESCE(it."std_gas_used", 0) AS "std_gas_used",
  COALESCE(ot."out_trace_count", 0) AS "out_trace_count",
  COALESCE(ot."out_addr_count", 0) AS "out_addr_count",
  COALESCE(ot."out_transfer_count", 0) AS "out_transfer_count",
  COALESCE(ot."out_avg_amount", 0) AS "out_avg_amount",
  COALESCE(ti."token_in_tnx", 0) AS "token_in_tnx",
  COALESCE(ti."token_in_type", 0) AS "token_in_type",
  COALESCE(ti."token_from_addr", 0) AS "token_from_addr",
  COALESCE(to2."token_out_tnx", 0) AS "token_out_tnx",
  COALESCE(to2."token_out_type", 0) AS "token_out_type",
  COALESCE(to2."token_to_addr", 0) AS "token_to_addr",
  COALESCE(mr."reward_amount", 0) AS "reward_amount",
  COALESCE(cc."contract_create_count", 0) AS "contract_create_count",
  COALESCE(fs."failure_count", 0) AS "failure_count",
  COALESCE(bs."bytecode_size", 0) AS "bytecode_size"
FROM active_addresses a
LEFT JOIN balance_summary b ON a."address" = b."address"
LEFT JOIN activity_pattern ap ON a."address" = ap."address"
LEFT JOIN incoming_traces it ON a."address" = it."address"
LEFT JOIN outgoing_traces ot ON a."address" = ot."address"
LEFT JOIN token_in ti ON a."address" = ti."address"
LEFT JOIN token_out to2 ON a."address" = to2."address"
LEFT JOIN mining_rewards mr ON a."address" = mr."address"
LEFT JOIN contract_creation cc ON a."address" = cc."address"
LEFT JOIN failure_summary fs ON a."address" = fs."address"
LEFT JOIN bytecode_sizes bs ON a."address" = bs."address"
WHERE (it."in_trace_count" > 0 OR ot."out_trace_count" > 0 OR ti."token_in_tnx" > 0 OR to2."token_out_tnx" > 0 OR cc."contract_create_count" > 0 OR mr."reward_amount" > 0)
ORDER BY a."address"