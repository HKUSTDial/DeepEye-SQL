SELECT
    "block_number",
    SUBSTR(PARSE_JSON("topics")[1]::STRING, 3) AS "source_address",
    HEXTOTRON(SUBSTR(PARSE_JSON("topics")[2]::STRING, 3)) AS "destination_address_tronlink",
    TRY_TO_NUMBER(SUBSTR("data", 3), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') / 1000000 AS "transfer_amount"
FROM
    "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_TRON_MAINNET_US"."LOGS"
WHERE
    "address" = '0xa614f803b6fd780986a42c78ec9c7f77e6ded13c'
    AND PARSE_JSON("topics")[0]::STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND "removed" = FALSE
ORDER BY
    "transfer_amount" DESC
LIMIT 3