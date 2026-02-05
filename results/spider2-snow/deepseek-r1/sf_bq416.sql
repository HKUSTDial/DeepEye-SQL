WITH logs_parsed AS (
  SELECT 
    "block_number",
    PARSE_JSON("topics") AS topics_array,
    "data"
  FROM "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_TRON_MAINNET_US"."LOGS"
  WHERE "address" = '0xa614f803b6fd780986a42c78ec9c7f77e6ded13c'
    AND topics_array[0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
)
SELECT 
  "block_number",
  topics_array[1] AS source_address,
  HEX_TO_TRON(topics_array[2]) AS destination_address_tronlink,
  TO_NUMBER(LPAD(SUBSTR("data", 3), 64, '0'), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') / 1000000 AS transfer_amount
FROM logs_parsed
ORDER BY transfer_amount DESC
LIMIT 3