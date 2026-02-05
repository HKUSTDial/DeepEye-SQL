WITH april_txs AS (
    SELECT "hash"
    FROM "CRYPTO"."CRYPTO_DASH"."TRANSACTIONS"
    WHERE "block_timestamp_month" = DATE '2023-04-01'
),
input_data AS (
    SELECT
        f.value::TEXT AS "address",
        -i."value" AS "value"
    FROM "CRYPTO"."CRYPTO_DASH"."INPUTS" i
    INNER JOIN april_txs t ON i."transaction_hash" = t."hash"
    JOIN LATERAL FLATTEN(input => i."addresses") f
    WHERE i."addresses" IS NOT NULL
),
output_data AS (
    SELECT
        f.value::TEXT AS "address",
        o."value" AS "value"
    FROM "CRYPTO"."CRYPTO_DASH"."OUTPUTS" o
    INNER JOIN april_txs t ON o."transaction_hash" = t."hash"
    JOIN LATERAL FLATTEN(input => o."addresses") f
    WHERE o."addresses" IS NOT NULL
),
combined AS (
    SELECT "address", "value" FROM input_data
    UNION ALL
    SELECT "address", "value" FROM output_data
),
balances AS (
    SELECT
        "address",
        SUM("value") AS "balance"
    FROM combined
    GROUP BY "address"
)
SELECT
    MAX("balance") AS "highest_balance",
    MIN("balance") AS "lowest_balance"
FROM balances