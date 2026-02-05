SELECT
  t.OrderID AS tradeID,
  t.MaturityDate AS tradeTimestamp,
  CASE
    WHEN SUBSTR(t.TargetCompID, 1, 4) = 'MOMO' THEN 'Momentum'
    WHEN SUBSTR(t.TargetCompID, 1, 4) = 'LUCK' THEN 'Feeling Lucky'
    WHEN SUBSTR(t.TargetCompID, 1, 4) = 'PRED' THEN 'Prediction'
    ELSE NULL
  END AS algorithm,
  t.Symbol AS symbol,
  t.LastPx AS openPrice,
  t.StrikePrice AS closePrice,
  Sides.Side AS tradeDirection,
  CASE
    WHEN Sides.Side = 'SHORT' THEN -1
    WHEN Sides.Side = 'LONG' THEN 1
    ELSE NULL
  END AS tradeMultiplier
FROM `bigquery-public-data.cymbal_investments.trade_capture_report` AS t
LEFT JOIN UNNEST(t.Sides) AS Sides
ORDER BY t.StrikePrice DESC
LIMIT 6