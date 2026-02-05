WITH oct2017_transactions AS (
    SELECT 
        f.value::STRING AS address,
        TO_TIMESTAMP(i."block_timestamp" / 1000000) AS transaction_timestamp,
        DATE(transaction_timestamp) AS transaction_date,
        i."value" AS amount
    FROM "CRYPTO"."CRYPTO_BITCOIN"."INPUTS" AS i,
    LATERAL FLATTEN(INPUT => i."addresses") AS f
    WHERE DATE(TO_TIMESTAMP(i."block_timestamp" / 1000000)) BETWEEN '2017-10-01' AND '2017-10-31'
    UNION ALL
    SELECT 
        f.value::STRING AS address,
        TO_TIMESTAMP(o."block_timestamp" / 1000000) AS transaction_timestamp,
        DATE(transaction_timestamp) AS transaction_date,
        o."value" AS amount
    FROM "CRYPTO"."CRYPTO_BITCOIN"."OUTPUTS" AS o,
    LATERAL FLATTEN(INPUT => o."addresses") AS f
    WHERE DATE(TO_TIMESTAMP(o."block_timestamp" / 1000000)) BETWEEN '2017-10-01' AND '2017-10-31'
),
address_stats AS (
    SELECT 
        address,
        MAX(transaction_date) AS latest_date,
        SUM(amount) AS total_value
    FROM oct2017_transactions
    GROUP BY address
),
max_date AS (
    SELECT MAX(latest_date) AS max_latest_date
    FROM address_stats
)
SELECT 
    address,
    latest_date,
    total_value
FROM address_stats
WHERE latest_date = (SELECT max_latest_date FROM max_date)
ORDER BY total_value DESC
LIMIT 1