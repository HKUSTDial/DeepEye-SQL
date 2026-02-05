SELECT 
  "OrderID" AS tradeID,
  TO_TIMESTAMP("MaturityDate" / 1000000) AS tradeTimestamp,
  CASE 
    WHEN LEFT("TargetCompID", 4) = 'MOMO' THEN 'Momentum'
    WHEN LEFT("TargetCompID", 4) = 'LUCK' THEN 'Feeling Lucky'
    WHEN LEFT("TargetCompID", 4) = 'PRED' THEN 'Prediction'
    ELSE 'Unknown'
  END AS algorithm,
  "Symbol" AS symbol,
  "LastPx" AS openPrice,
  "StrikePrice" AS closePrice,
  f.value AS tradeDirection,
  CASE 
    WHEN f.value = 'SHORT' THEN -1
    WHEN f.value = 'LONG' THEN 1
    ELSE 0
  END AS tradeMultiplier
FROM "CYMBAL_INVESTMENTS"."CYMBAL_INVESTMENTS"."TRADE_CAPTURE_REPORT"
LEFT JOIN LATERAL FLATTEN(INPUT => "Sides") f
QUALIFY ROW_NUMBER() OVER (PARTITION BY "TradeReportID" ORDER BY f.index) = 1
ORDER BY "StrikePrice" DESC
LIMIT 6