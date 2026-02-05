WITH magnificent_tickers AS (
    SELECT 'AAPL' AS "TICKER" UNION ALL
    SELECT 'MSFT' UNION ALL
    SELECT 'GOOGL' UNION ALL
    SELECT 'GOOG' UNION ALL
    SELECT 'AMZN' UNION ALL
    SELECT 'NVDA' UNION ALL
    SELECT 'META' UNION ALL
    SELECT 'TSLA'
), price_jan1 AS (
    SELECT 
        s."TICKER",
        s."VALUE" AS "price_jan1",
        s."DATE" AS "date_jan1"
    FROM "FINANCE__ECONOMICS"."CYBERSYN"."STOCK_PRICE_TIMESERIES" s
    INNER JOIN magnificent_tickers m ON s."TICKER" = m."TICKER"
    WHERE s."VARIABLE_NAME" = 'Post-Market Close'
      AND s."DATE" <= '2024-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s."TICKER" ORDER BY s."DATE" DESC) = 1
), price_jun30 AS (
    SELECT 
        s."TICKER",
        s."VALUE" AS "price_jun30",
        s."DATE" AS "date_jun30"
    FROM "FINANCE__ECONOMICS"."CYBERSYN"."STOCK_PRICE_TIMESERIES" s
    INNER JOIN magnificent_tickers m ON s."TICKER" = m."TICKER"
    WHERE s."VARIABLE_NAME" = 'Post-Market Close'
      AND s."DATE" <= '2024-06-30'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s."TICKER" ORDER BY s."DATE" DESC) = 1
)
SELECT 
    p1."TICKER",
    p1."price_jan1",
    p2."price_jun30",
    (p2."price_jun30" - p1."price_jan1") / p1."price_jan1" * 100 AS "percentage_change"
FROM price_jan1 p1
INNER JOIN price_jun30 p2 ON p1."TICKER" = p2."TICKER"
ORDER BY p1."TICKER"