WITH date_range AS (
  SELECT DATEADD('day', SEQ4(), '2018-08-30'::DATE) AS "date"
  FROM TABLE(GENERATOR(ROWCOUNT => 32))
  WHERE "date" <= '2018-09-30'::DATE
),
daily_counts AS (
  SELECT
    DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) AS "date",
    SUM(CASE WHEN "trace_address" IS NULL THEN 1 ELSE 0 END) AS external_count,
    SUM(CASE WHEN "trace_address" IS NOT NULL THEN 1 ELSE 0 END) AS internal_count
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
  WHERE "trace_type" = 'create'
    AND DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) BETWEEN '2018-08-30' AND '2018-09-30'
  GROUP BY DATE(TO_TIMESTAMP("block_timestamp" / 1000000))
),
combined AS (
  SELECT
    d."date",
    COALESCE(dc.external_count, 0) AS external_count,
    COALESCE(dc.internal_count, 0) AS internal_count
  FROM date_range d
  LEFT JOIN daily_counts dc ON d."date" = dc."date"
)
SELECT
  "date",
  SUM(external_count) OVER (ORDER BY "date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_external_creates,
  SUM(internal_count) OVER (ORDER BY "date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_internal_creates
FROM combined
ORDER BY "date"