SELECT
    "TRANSACTION_HASH" AS "transaction_hash",
    'https://optimistic.etherscan.io/tx/' || SUBSTR("TRANSACTION_HASH", 3) AS "etherscan_link",
    SUBSTR(GET(GET("TOPICS", 1), 'utf8'), 3) AS "l1_token_address",
    SUBSTR(GET(GET("TOPICS", 2), 'utf8'), 3) AS "l2_token_address",
    SUBSTR(GET(GET("TOPICS", 3), 'utf8'), 3) AS "sender_address",
    SUBSTR(SPLIT_PART("ARGS", ',', 1), 3) AS "receiver_address",
    TO_NUMBER(SUBSTR(SPLIT_PART("ARGS", ',', 2), 3), 'XXXXXXXXXXXXXXX') AS "deposited_amount"
FROM
    "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_ARBITRUM_ONE_US"."DECODED_EVENTS"
WHERE
    "BLOCK_NUMBER" = 29815485
    AND "EVENT_HASH" = '0x3303facd24627943a92e9dc87cfbb34b15c49b726eec3ad3487c16be9ab8efe8'
    AND "REMOVED" = FALSE
    AND GET("TOPICS", 0)::STRING = '0x3303facd24627943a92e9dc87cfbb34b15c49b726eec3ad3487c16be9ab8efe8'