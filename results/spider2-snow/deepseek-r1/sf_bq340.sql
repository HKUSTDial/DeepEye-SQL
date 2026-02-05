WITH token_transfers_unpivot AS (
  SELECT 
    "from_address" AS "address",
    "token_address",
    -TO_NUMERIC("value") AS "value_change"
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TOKEN_TRANSFERS"
  WHERE "token_address" IN ('0x0d8775f648430679a709e98d2b0cb6250d2887ef0', '0x1e15c05cbad367f044cbfbafda3d9a1510db5513')
  UNION ALL
  SELECT 
    "to_address" AS "address",
    "token_address",
    TO_NUMERIC("value") AS "value_change"
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."TOKEN_TRANSFERS"
  WHERE "token_address" IN ('0x0d8775f648430679a709e98d2b0cb6250d2887ef0', '0x1e15c05cbad367f044cbfbafda3d9a1510db5513')
),
net_changes AS (
  SELECT 
    "address",
    "token_address",
    SUM("value_change") AS "net_change"
  FROM token_transfers_unpivot
  GROUP BY "address", "token_address"
),
combined_abs_changes AS (
  SELECT 
    "address",
    SUM(ABS("net_change")) AS "total_absolute_change"
  FROM net_changes
  WHERE "address" != '0x0000000000000000000000000000000000000000'
  GROUP BY "address"
)
SELECT "address"
FROM combined_abs_changes
ORDER BY "total_absolute_change" DESC
LIMIT 6