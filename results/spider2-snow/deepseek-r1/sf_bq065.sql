WITH recent_requests AS (
  SELECT 
    "block_timestamp",
    "oracle_request_id",
    "decoded_result",
    "request"
  FROM "CRYPTO"."CRYPTO_BAND"."ORACLE_REQUESTS"
  WHERE "oracle_script":id::NUMBER = 3
  ORDER BY TO_TIMESTAMP("block_timestamp") DESC
  LIMIT 10
)
SELECT 
  r."block_timestamp",
  r."oracle_request_id",
  f.value::STRING AS "symbol",
  (r."decoded_result":rates[f.index]::NUMBER / r."request":multiplier::NUMBER) AS "adjusted_rate"
FROM recent_requests r,
LATERAL FLATTEN(INPUT => r."decoded_result":symbols) f
ORDER BY TO_TIMESTAMP(r."block_timestamp") DESC