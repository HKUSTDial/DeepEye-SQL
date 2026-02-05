WITH successful_txs AS (
    SELECT "hash", "from_address"
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
    WHERE "receipt_status" = 1
        AND "block_timestamp" < 1630454400000000
), excluded_traces AS (
    SELECT DISTINCT "transaction_hash"
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
    WHERE "call_type" IN ('delegatecall', 'callcode', 'staticcall')
), tx_counts AS (
    SELECT st."from_address", COUNT(*) AS tx_count
    FROM successful_txs st
    WHERE st."hash" NOT IN (SELECT "transaction_hash" FROM excluded_traces)
    GROUP BY st."from_address"
), top_address AS (
    SELECT "from_address" AS address
    FROM tx_counts
    ORDER BY tx_count DESC
    LIMIT 1
)
SELECT 
    ta.address,
    ( 
        COALESCE((SELECT SUM("value") FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS" WHERE "to_address" = ta.address AND "receipt_status" = 1 AND "block_timestamp" < 1630454400000000), 0)
        + COALESCE((SELECT SUM("value") FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES" WHERE "to_address" = ta.address AND "status" = 1 AND "block_timestamp" < 1630454400000000), 0)
        + COALESCE((SELECT SUM("value") FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES" WHERE "trace_type" = 'reward' AND "to_address" = ta.address AND "block_timestamp" < 1630454400000000), 0)
        - COALESCE((SELECT SUM("value") FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS" WHERE "from_address" = ta.address AND "receipt_status" = 1 AND "block_timestamp" < 1630454400000000), 0)
        - COALESCE((SELECT SUM("value") FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES" WHERE "from_address" = ta.address AND "status" = 1 AND "block_timestamp" < 1630454400000000), 0)
        - COALESCE((SELECT SUM("receipt_effective_gas_price" * "receipt_gas_used") FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS" WHERE "from_address" = ta.address AND "block_timestamp" < 1630454400000000), 0)
    ) / 1000000000000000000 AS balance_ether
FROM top_address ta;