WITH "months" AS (
  SELECT 2023 AS "year", SEQ4() + 1 AS "month"
  FROM TABLE(GENERATOR(ROWCOUNT => 12))
), "all_transactions" AS (
  SELECT "block_timestamp"
  FROM "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_CRONOS_MAINNET_US"."TRANSACTIONS"
  WHERE "block_timestamp" >= '2023-01-01' AND "block_timestamp" < '2024-01-01'
  UNION ALL
  SELECT "block_timestamp"
  FROM "GOOG_BLOCKCHAIN"."GOOG_BLOCKCHAIN_POLYGON_MAINNET_US"."TRANSACTIONS"
  WHERE "block_timestamp" >= '2023-01-01' AND "block_timestamp" < '2024-01-01'
), "monthly_counts" AS (
  SELECT
    EXTRACT(YEAR FROM "block_timestamp") AS "year",
    EXTRACT(MONTH FROM "block_timestamp") AS "month",
    COUNT(*) AS "monthly_count"
  FROM "all_transactions"
  GROUP BY "year", "month"
)
SELECT
  "m"."year",
  "m"."month",
  COALESCE("mc"."monthly_count", 0) AS "monthly_count",
  COALESCE("mc"."monthly_count", 0) / DATEDIFF('second', DATE_FROM_PARTS("m"."year", "m"."month", 1), DATEADD('month', 1, DATE_FROM_PARTS("m"."year", "m"."month", 1))) AS "transactions_per_second"
FROM "months" "m"
LEFT JOIN "monthly_counts" "mc" ON "m"."year" = "mc"."year" AND "m"."month" = "mc"."month"
ORDER BY "monthly_count" DESC