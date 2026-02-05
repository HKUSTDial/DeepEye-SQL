WITH filtered_transactions AS (
    SELECT 
        t."from_address",
        COALESCE(t."to_address", t."receipt_contract_address") as "receiver_address",
        b."miner" as "miner_address",
        t."value",
        t."gas_price",
        t."receipt_gas_used",
        (t."gas_price" * t."receipt_gas_used") as "gas_fee"
    FROM "CRYPTO"."CRYPTO_ETHEREUM_CLASSIC"."TRANSACTIONS" t
    JOIN "CRYPTO"."CRYPTO_ETHEREUM_CLASSIC"."BLOCKS" b
        ON t."block_number" = b."number"
    WHERE t."block_timestamp" >= 1476403200000000 
        AND t."block_timestamp" < 1476489600000000
        AND t."receipt_status" = 1
), contributions AS (
    SELECT "from_address" as "address", (- "value" - "gas_fee") as "delta"
    FROM filtered_transactions
    UNION ALL
    SELECT "receiver_address" as "address", "value" as "delta"
    FROM filtered_transactions
    WHERE "receiver_address" IS NOT NULL
    UNION ALL
    SELECT "miner_address" as "address", "gas_fee" as "delta"
    FROM filtered_transactions
), address_net AS (
    SELECT "address", SUM("delta") as "net_balance_change"
    FROM contributions
    GROUP BY "address"
)
SELECT MAX("net_balance_change") as "max_net_balance_change", MIN("net_balance_change") as "min_net_balance_change"
FROM address_net