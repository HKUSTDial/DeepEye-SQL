SELECT 'https://cronoscan.com/address/' || "from_address" AS sender_url
FROM (
    SELECT t."from_address", COUNT(*) AS tx_count
    FROM "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_CRONOS_MAINNET_US"."TRANSACTIONS" t
    INNER JOIN "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_CRONOS_MAINNET_US"."BLOCKS" b
    ON t."block_hash" = b."block_hash"
    WHERE t."to_address" IS NOT NULL
    AND b."size" > 4096
    AND b."block_timestamp" >= '2023-01-01'::TIMESTAMP_NTZ
    GROUP BY t."from_address"
    ORDER BY tx_count DESC
    LIMIT 1
)