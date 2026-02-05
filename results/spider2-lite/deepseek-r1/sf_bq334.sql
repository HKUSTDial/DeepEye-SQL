WITH combined AS (
  SELECT "value", "block_timestamp", 'output' AS source FROM "CRYPTO"."CRYPTO_BITCOIN"."OUTPUTS"
  UNION ALL
  SELECT "value", "block_timestamp", 'input' AS source FROM "CRYPTO"."CRYPTO_BITCOIN"."INPUTS"
), merged_outputs AS (
  SELECT 
    DATE_PART('year', TO_TIMESTAMP("block_timestamp" / 1000000)) AS "year",
    AVG("value") AS "avg_merged_output_value"
  FROM combined
  WHERE source = 'output'
  GROUP BY "year"
), transactions_avg AS (
  SELECT 
    DATE_PART('year', TO_TIMESTAMP("block_timestamp" / 1000000)) AS "year",
    AVG("output_value") AS "avg_tx_output_value"
  FROM "CRYPTO"."CRYPTO_BITCOIN"."TRANSACTIONS"
  GROUP BY "year"
)
SELECT 
  m."year",
  m."avg_merged_output_value" - t."avg_tx_output_value" AS "difference"
FROM merged_outputs m
INNER JOIN transactions_avg t ON m."year" = t."year"
ORDER BY m."year"