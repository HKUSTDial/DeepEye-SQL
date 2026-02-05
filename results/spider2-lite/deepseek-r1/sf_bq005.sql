WITH blocks_with_row AS (
    SELECT 
        "number",
        "timestamp",
        DATE(TO_TIMESTAMP("timestamp" / 1000000)) AS block_date,
        ROW_NUMBER() OVER (ORDER BY "number") AS rn
    FROM "CRYPTO"."CRYPTO_BITCOIN"."BLOCKS"
    WHERE "number" != 0
),
consecutive_pairs AS (
    SELECT
        prev.block_date AS prev_date,
        (next."timestamp" - prev."timestamp") / 1000000 AS interval_seconds
    FROM blocks_with_row AS prev
    INNER JOIN blocks_with_row AS next
        ON prev.rn = next.rn - 1
)
SELECT
    prev_date AS date,
    AVG(interval_seconds) AS avg_block_interval_seconds
FROM consecutive_pairs
WHERE prev_date BETWEEN DATE '2023-01-01' AND DATE '2023-12-31'
GROUP BY prev_date
ORDER BY prev_date
LIMIT 10