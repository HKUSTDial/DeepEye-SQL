WITH recent_requests AS (
    SELECT 
        "oracle_request_id",
        "block_timestamp",
        "request",
        "decoded_result"
    FROM "CRYPTO"."CRYPTO_BAND"."ORACLE_REQUESTS"
    WHERE "oracle_script":"id" = 3
    ORDER BY "block_timestamp" DESC
    LIMIT 10
)
SELECT 
    rr."block_timestamp",
    rr."oracle_request_id",
    symbol.value::STRING AS "symbol",
    (rr."decoded_result":"rates"[symbol.index]::FLOAT / rr."request":"multiplier"::FLOAT) AS "adjusted_rate"
FROM recent_requests rr,
LATERAL FLATTEN(INPUT => rr."decoded_result":"symbols") symbol
ORDER BY rr."block_timestamp" DESC