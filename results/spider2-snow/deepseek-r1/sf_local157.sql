WITH converted_data AS (
  SELECT 
    "ticker",
    "market_date",
    TO_DATE("market_date", 'DD-MM-YYYY') AS trade_date,
    CASE 
      WHEN TRIM("volume") = '-' THEN 0
      WHEN RIGHT(TRIM("volume"), 1) = 'K' THEN TRY_CAST(LEFT(TRIM("volume"), LENGTH(TRIM("volume")) - 1) AS FLOAT) * 1000
      WHEN RIGHT(TRIM("volume"), 1) = 'M' THEN TRY_CAST(LEFT(TRIM("volume"), LENGTH(TRIM("volume")) - 1) AS FLOAT) * 1000000
      ELSE COALESCE(TRY_CAST(TRIM("volume") AS FLOAT), 0)
    END AS volume_numeric
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."BITCOIN_PRICES"
  WHERE TO_DATE("market_date", 'DD-MM-YYYY') BETWEEN '2021-08-01' AND '2021-08-10'
),
lagged_data AS (
  SELECT 
    "ticker",
    "market_date",
    trade_date,
    volume_numeric,
    LAG(volume_numeric) OVER (PARTITION BY "ticker" ORDER BY trade_date) AS prev_volume
  FROM converted_data
)
SELECT 
  "ticker",
  "market_date",
  CASE 
    WHEN prev_volume > 0 THEN (volume_numeric - prev_volume) / prev_volume * 100
    ELSE NULL
  END AS volume_pct_change
FROM lagged_data
ORDER BY "ticker", trade_date;