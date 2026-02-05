WITH feeling_lucky AS (
  SELECT AVG(`StrikePrice` - `LastPx`) AS avg_intrinsic
  FROM `bigquery-public-data.cymbal_investments.trade_capture_report`
  WHERE EXISTS (
    SELECT 1
    FROM UNNEST(`Sides`) AS side
    WHERE side.`Side` = 'Buy'
      AND EXISTS (
        SELECT 1
        FROM UNNEST(side.`PartyIDs`) AS party
        WHERE party.`PartyRole` = 'Strategy' AND party.`PartyID` = 'feeling-lucky'
      )
  )
),
momentum AS (
  SELECT AVG(`StrikePrice` - `LastPx`) AS avg_intrinsic
  FROM `bigquery-public-data.cymbal_investments.trade_capture_report`
  WHERE EXISTS (
    SELECT 1
    FROM UNNEST(`Sides`) AS side
    WHERE side.`Side` = 'Buy'
      AND EXISTS (
        SELECT 1
        FROM UNNEST(side.`PartyIDs`) AS party
        WHERE party.`PartyRole` = 'Strategy' AND party.`PartyID` = 'momentum'
      )
  )
)
SELECT feeling_lucky.avg_intrinsic - momentum.avg_intrinsic AS higher_avg_intrinsic_value
FROM feeling_lucky, momentum