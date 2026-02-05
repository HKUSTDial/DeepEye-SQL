WITH mag7_tickers AS (
  SELECT 'AAPL' AS "TICKER" UNION ALL
  SELECT 'MSFT' UNION ALL
  SELECT 'GOOGL' UNION ALL
  SELECT 'AMZN' UNION ALL
  SELECT 'NVDA' UNION ALL
  SELECT 'TSLA' UNION ALL
  SELECT 'META'
),
jan_prices AS (
  SELECT 
    s."TICKER",
    s."VALUE" AS "price_jan1",
    s."DATE" AS "jan_date"
  FROM "FINANCE__ECONOMICS"."CYBERSYN"."STOCK_PRICE_TIMESERIES" s
  INNER JOIN mag7_tickers m ON s."TICKER" = m."TICKER"
  WHERE s."VARIABLE_NAME" = 'Post-Market Close'
    AND s."DATE" <= '2024-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY s."TICKER" ORDER BY s."DATE" DESC) = 1
),
june_prices AS (
  SELECT 
    s."TICKER",
    s."VALUE" AS "price_june30",
    s."DATE" AS "june_date"
  FROM "FINANCE__ECONOMICS"."CYBERSYN"."STOCK_PRICE_TIMESERIES" s
  INNER JOIN mag7_tickers m ON s."TICKER" = m."TICKER"
  WHERE s."VARIABLE_NAME" = 'Post-Market Close'
    AND s."DATE" <= '2024-06-30'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY s."TICKER" ORDER BY s."DATE" DESC) = 1
),
percentage_change AS (
  SELECT 
    jp."TICKER",
    jp."price_jan1",
    jun."price_june30",
    ((jun."price_june30" - jp."price_jan1") / jp."price_jan1") * 100 AS "pct_change"
  FROM jan_prices jp
  INNER JOIN june_prices jun ON jp."TICKER" = jun."TICKER"
)
SELECT 
  ci."COMPANY_NAME",
  pc."TICKER",
  pc."pct_change" AS "percentage_change"
FROM percentage_change pc
LEFT JOIN "FINANCE__ECONOMICS"."CYBERSYN"."COMPANY_INDEX" ci ON pc."TICKER" = ci."PRIMARY_TICKER"
ORDER BY pc."TICKER"