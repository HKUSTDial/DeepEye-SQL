SELECT 'https://cronoscan.com/address/' || t."from_address" AS "sender_url"
FROM "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_CRONOS_MAINNET_US"."TRANSACTIONS" t
INNER JOIN "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_CRONOS_MAINNET_US"."BLOCKS" b
    ON t."block_hash" = b."block_hash"
WHERE b."size" > 4096
    AND b."block_timestamp" >= '2023-01-01'
    AND t."to_address" IS NOT NULL
GROUP BY t."from_address"
ORDER BY COUNT(*) DESC
LIMIT 1