SELECT
    "address",
    SUM("balance_change") AS "total_balance"
FROM (
    SELECT
        "from_address" AS "address",
        -("receipt_effective_gas_price" * "receipt_gas_used") AS "balance_change"
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
    WHERE "block_timestamp" < 1630454400000000
        AND "receipt_status" = 1
        AND "from_address" IS NOT NULL
    UNION ALL
    SELECT
        "from_address" AS "address",
        -"value" AS "balance_change"
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
    WHERE "block_timestamp" < 1630454400000000
        AND "status" = 1
        AND ("call_type" IS NULL OR "call_type" = 'call')
        AND "from_address" IS NOT NULL
    UNION ALL
    SELECT
        "to_address" AS "address",
        "value" AS "balance_change"
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
    WHERE "block_timestamp" < 1630454400000000
        AND "status" = 1
        AND ("call_type" IS NULL OR "call_type" = 'call')
        AND "to_address" IS NOT NULL
) "combined"
GROUP BY "address"
ORDER BY "total_balance" DESC
LIMIT 10