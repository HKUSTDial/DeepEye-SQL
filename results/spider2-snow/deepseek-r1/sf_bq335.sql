WITH october_transactions AS (
    SELECT 
        addr.value::STRING AS address,
        DATE(TO_TIMESTAMP(i."block_timestamp" / 1000000)) AS transaction_date,
        i."value" AS amount
    FROM "CRYPTO"."CRYPTO_BITCOIN"."INPUTS" i
    JOIN LATERAL FLATTEN(INPUT => i."addresses") addr
    WHERE i."block_timestamp" >= 1506816000000000 
      AND i."block_timestamp" < 1509494400000000
    UNION ALL
    SELECT 
        addr.value::STRING AS address,
        DATE(TO_TIMESTAMP(o."block_timestamp" / 1000000)) AS transaction_date,
        o."value" AS amount
    FROM "CRYPTO"."CRYPTO_BITCOIN"."OUTPUTS" o
    JOIN LATERAL FLATTEN(INPUT => o."addresses") addr
    WHERE o."block_timestamp" >= 1506816000000000 
      AND o."block_timestamp" < 1509494400000000
),
address_stats AS (
    SELECT 
        address,
        MAX(transaction_date) AS last_transaction_date,
        SUM(amount) AS total_value
    FROM october_transactions
    GROUP BY address
)
SELECT 
    address,
    last_transaction_date,
    total_value
FROM address_stats
WHERE last_transaction_date = (SELECT MAX(last_transaction_date) FROM address_stats)
ORDER BY total_value DESC
LIMIT 1