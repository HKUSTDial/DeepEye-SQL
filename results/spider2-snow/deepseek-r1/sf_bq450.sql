WITH active_addresses AS (
  SELECT DISTINCT "address" FROM (
    SELECT "from_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" WHERE "block_timestamp" < 1483228800000000
    UNION
    SELECT "to_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" WHERE "block_timestamp" < 1483228800000000
    UNION
    SELECT "from_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" WHERE "block_timestamp" < 1483228800000000
    UNION
    SELECT "to_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" WHERE "block_timestamp" < 1483228800000000
    UNION
    SELECT "from_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS" WHERE "block_timestamp" < 1483228800000000
    UNION
    SELECT "to_address" AS "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS" WHERE "block_timestamp" < 1483228800000000
  ) WHERE "address" IS NOT NULL
),
eth_in AS (
  SELECT "to_address" AS "address", 
    SUM("value") AS "total_in_value"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "trace_type" = 'call'
    AND "status" = 1
    AND ("call_type" IS NULL OR "call_type" NOT IN ('delegatecall', 'callcode', 'staticcall'))
    AND "block_timestamp" < 1483228800000000
  GROUP BY "to_address"
),
eth_out AS (
  SELECT "from_address" AS "address", 
    SUM("value") AS "total_out_value"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "trace_type" = 'call'
    AND "status" = 1
    AND ("call_type" IS NULL OR "call_type" NOT IN ('delegatecall', 'callcode', 'staticcall'))
    AND "block_timestamp" < 1483228800000000
  GROUP BY "from_address"
),
tx_fees AS (
  SELECT "from_address" AS "address",
    SUM("receipt_gas_used" * "gas_price") AS "total_fees"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
  WHERE "receipt_status" = 1
    AND "block_timestamp" < 1483228800000000
  GROUP BY "from_address"
),
all_activities AS (
  SELECT "address", 
    TO_TIMESTAMP("ts" / 1000000) AS "activity_time"
  FROM (
    SELECT "from_address" AS "address", "block_timestamp" AS "ts" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" WHERE "block_timestamp" < 1483228800000000 AND "from_address" IS NOT NULL
    UNION ALL
    SELECT "to_address" AS "address", "block_timestamp" AS "ts" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" WHERE "block_timestamp" < 1483228800000000 AND "to_address" IS NOT NULL
    UNION ALL
    SELECT "from_address" AS "address", "block_timestamp" AS "ts" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" WHERE "block_timestamp" < 1483228800000000 AND "from_address" IS NOT NULL
    UNION ALL
    SELECT "to_address" AS "address", "block_timestamp" AS "ts" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS" WHERE "block_timestamp" < 1483228800000000 AND "to_address" IS NOT NULL
    UNION ALL
    SELECT "from_address" AS "address", "block_timestamp" AS "ts" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS" WHERE "block_timestamp" < 1483228800000000 AND "from_address" IS NOT NULL
    UNION ALL
    SELECT "to_address" AS "address", "block_timestamp" AS "ts" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS" WHERE "block_timestamp" < 1483228800000000 AND "to_address" IS NOT NULL
  )
),
activity_patterns AS (
  SELECT "address",
    COUNT(DISTINCT DATE("activity_time")) AS "active_days",
    COUNT(*) AS "n_activities",
    CASE 
      WHEN COUNT(*) > 24 THEN 
        SQRT(POW(SUM(COS(2 * PI() * EXTRACT(HOUR FROM "activity_time") / 24)), 2) + 
             POW(SUM(SIN(2 * PI() * EXTRACT(HOUR FROM "activity_time") / 24)), 2)) / COUNT(*)
      ELSE NULL
    END AS "R_active_hour"
  FROM all_activities
  GROUP BY "address"
),
in_traces AS (
  SELECT "to_address" AS "address",
    COUNT(*) AS "in_trace_count",
    COUNT(DISTINCT "from_address") AS "in_addr_count",
    SUM(CASE WHEN "value" > 0 THEN 1 ELSE 0 END) AS "in_transfer_count",
    AVG(CASE WHEN "value" > 0 THEN "value" / 1e18 END) AS "in_avg_amount",
    AVG("gas_used") AS "avg_gas_used",
    STDDEV("gas_used") AS "std_gas_used"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "trace_type" = 'call'
    AND "status" = 1
    AND ("call_type" IS NULL OR "call_type" NOT IN ('delegatecall', 'callcode', 'staticcall'))
    AND "block_timestamp" < 1483228800000000
  GROUP BY "to_address"
),
out_traces AS (
  SELECT "from_address" AS "address",
    COUNT(*) AS "out_trace_count",
    COUNT(DISTINCT "to_address") AS "out_addr_count",
    SUM(CASE WHEN "value" > 0 THEN 1 ELSE 0 END) AS "out_transfer_count",
    AVG(CASE WHEN "value" > 0 THEN "value" / 1e18 END) AS "out_avg_amount"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "trace_type" = 'call'
    AND "status" = 1
    AND ("call_type" IS NULL OR "call_type" NOT IN ('delegatecall', 'callcode', 'staticcall'))
    AND "block_timestamp" < 1483228800000000
  GROUP BY "from_address"
),
token_in AS (
  SELECT "to_address" AS "address",
    COUNT(*) AS "token_in_tnx",
    COUNT(DISTINCT "token_address") AS "token_in_type",
    COUNT(DISTINCT "from_address") AS "token_from_addr"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS"
  WHERE "block_timestamp" < 1483228800000000
  GROUP BY "to_address"
),
token_out AS (
  SELECT "from_address" AS "address",
    COUNT(*) AS "token_out_tnx",
    COUNT(DISTINCT "token_address") AS "token_out_type",
    COUNT(DISTINCT "to_address") AS "token_to_addr"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS"
  WHERE "block_timestamp" < 1483228800000000
  GROUP BY "from_address"
),
rewards AS (
  SELECT "to_address" AS "address",
    SUM("value") AS "total_reward"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "trace_type" = 'reward'
    AND "reward_type" = 'block'
    AND "block_timestamp" < 1483228800000000
  GROUP BY "to_address"
),
contract_creations AS (
  SELECT "from_address" AS "address",
    COUNT(*) AS "contract_create_count"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "trace_type" = 'create'
    AND "block_timestamp" < 1483228800000000
  GROUP BY "from_address"
),
failures AS (
  SELECT "from_address" AS "address",
    COUNT(*) AS "failure_count"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES"
  WHERE "status" = 0
    AND "block_timestamp" < 1483228800000000
  GROUP BY "from_address"
),
bytecode_sizes AS (
  SELECT t."from_address" AS "address",
    AVG(LENGTH(c."bytecode")) AS "bytecode_size"
  FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRACES" t
  INNER JOIN "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."CONTRACTS" c
    ON t."to_address" = c."address"
  WHERE t."trace_type" = 'create'
    AND t."block_timestamp" < 1483228800000000
    AND c."block_timestamp" < 1483228800000000
  GROUP BY t."from_address"
)
SELECT 
  a."address",
  COALESCE((ei."total_in_value" - eo."total_out_value" - tf."total_fees") / 1e18, 0) AS "balance",
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
  COALESCE(to_t."token_out_tnx", 0) AS "token_out_tnx",
  COALESCE(to_t."token_out_type", 0) AS "token_out_type",
  COALESCE(to_t."token_to_addr", 0) AS "token_to_addr",
  COALESCE(r."total_reward" / 1e18, 0) AS "reward_amount",
  COALESCE(cc."contract_create_count", 0) AS "contract_create_count",
  COALESCE(f."failure_count", 0) AS "failure_count",
  COALESCE(bs."bytecode_size", 0) AS "bytecode_size"
FROM active_addresses a
LEFT JOIN eth_in ei ON a."address" = ei."address"
LEFT JOIN eth_out eo ON a."address" = eo."address"
LEFT JOIN tx_fees tf ON a."address" = tf."address"
LEFT JOIN activity_patterns ap ON a."address" = ap."address"
LEFT JOIN in_traces it ON a."address" = it."address"
LEFT JOIN out_traces ot ON a."address" = ot."address"
LEFT JOIN token_in ti ON a."address" = ti."address"
LEFT JOIN token_out to_t ON a."address" = to_t."address"
LEFT JOIN rewards r ON a."address" = r."address"
LEFT JOIN contract_creations cc ON a."address" = cc."address"
LEFT JOIN failures f ON a."address" = f."address"
LEFT JOIN bytecode_sizes bs ON a."address" = bs."address"
WHERE (it."in_trace_count" > 0 OR ot."out_trace_count" > 0 OR ti."token_in_tnx" > 0 OR to_t."token_out_tnx" > 0 OR cc."contract_create_count" > 0)
ORDER BY a."address"