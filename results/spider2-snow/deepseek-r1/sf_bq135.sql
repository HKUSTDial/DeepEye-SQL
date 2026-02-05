WITH transactions_with_date AS (
  SELECT 
    DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) AS "transaction_date",
    "amount"
  FROM "CRYPTO"."CRYPTO_ZILLIQA"."TRANSACTIONS"
  WHERE "block_timestamp" < 1640995200000000
    AND "success" = TRUE
    AND "accepted" = TRUE
)
SELECT 
  "transaction_date",
  SUM("amount") AS "total_amount"
FROM transactions_with_date
GROUP BY "transaction_date"
ORDER BY "total_amount" DESC
LIMIT 1