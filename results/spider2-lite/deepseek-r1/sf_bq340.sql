WITH token_transfers AS (
    SELECT 
        "from_address",
        "to_address",
        "token_address",
        TRY_TO_NUMERIC("value", 38, 0) AS value_num
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TOKEN_TRANSFERS"
    WHERE "token_address" IN ('0x0d8775f648430679a709e98d2b0cb6250d2887ef0', '0x1e15c05cbad367f044cbfbafda3d9a1510db5513')
),
address_flows AS (
    SELECT 
        address,
        "token_address",
        SUM(CASE WHEN direction = 'in' THEN value_num ELSE -value_num END) AS net_balance
    FROM (
        SELECT "from_address" AS address, "token_address", value_num, 'out' AS direction FROM token_transfers
        UNION ALL
        SELECT "to_address" AS address, "token_address", value_num, 'in' AS direction FROM token_transfers
    ) flows
    GROUP BY address, "token_address"
),
balances AS (
    SELECT 
        address,
        MAX(CASE WHEN "token_address" = '0x0d8775f648430679a709e98d2b0cb6250d2887ef0' THEN net_balance ELSE 0 END) AS balance_token1,
        MAX(CASE WHEN "token_address" = '0x1e15c05cbad367f044cbfbafda3d9a1510db5513' THEN net_balance ELSE 0 END) AS balance_token2
    FROM address_flows
    GROUP BY address
)
SELECT 
    address,
    ABS(balance_token1 - balance_token2) AS abs_diff
FROM balances
WHERE address != '0x0000000000000000000000000000000000000000'
ORDER BY abs_diff DESC
LIMIT 6