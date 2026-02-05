WITH daily_volumes AS (
    SELECT 
        ticker,
        market_date,
        (substr(market_date, 7, 4) || '-' || substr(market_date, 4, 2) || '-' || substr(market_date, 1, 2)) AS date_sortable,
        CASE 
            WHEN volume = '-' THEN 0.0
            WHEN volume LIKE '%K' THEN CAST(REPLACE(volume, 'K', '') AS REAL) * 1000
            WHEN volume LIKE '%M' THEN CAST(REPLACE(volume, 'M', '') AS REAL) * 1000000
            ELSE CAST(volume AS REAL)
        END AS converted_volume
    FROM bitcoin_prices
    WHERE (substr(market_date, 7, 4) || '-' || substr(market_date, 4, 2) || '-' || substr(market_date, 1, 2)) BETWEEN '2021-08-01' AND '2021-08-10'
)
SELECT 
    ticker,
    market_date,
    CASE 
        WHEN LAG(converted_volume, 1, 0) OVER (PARTITION BY ticker ORDER BY date_sortable) = 0 THEN NULL
        ELSE (converted_volume - LAG(converted_volume, 1, 0) OVER (PARTITION BY ticker ORDER BY date_sortable)) * 100.0 
             / LAG(converted_volume, 1, 0) OVER (PARTITION BY ticker ORDER BY date_sortable)
    END AS volume_change_percent
FROM daily_volumes
ORDER BY ticker, date_sortable