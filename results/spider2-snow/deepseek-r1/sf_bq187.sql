WITH bnb_token AS (
    SELECT "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKENS" WHERE "symbol" = 'BNB'
),
all_transfers AS (
    SELECT 
        "from_address",
        "to_address",
        TRY_CAST("value" AS NUMERIC(38,0)) AS value_num
    FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS"
    WHERE "token_address" IN (SELECT "address" FROM bnb_token)
      AND "from_address" != '0x0000000000000000000000000000000000000000'
      AND "to_address" != '0x0000000000000000000000000000000000000000'
),
address_inflows AS (
    SELECT 
        "to_address" AS address,
        SUM(value_num) AS inflow
    FROM all_transfers
    GROUP BY "to_address"
),
address_outflows AS (
    SELECT 
        "from_address" AS address,
        SUM(value_num) AS outflow
    FROM all_transfers
    GROUP BY "from_address"
),
address_balances AS (
    SELECT 
        COALESCE(i.address, o.address) AS address,
        COALESCE(inflow, 0) - COALESCE(outflow, 0) AS balance
    FROM address_inflows i
    FULL OUTER JOIN address_outflows o ON i.address = o.address
)
SELECT 
    SUM(balance) / POWER(10,18) AS total_circulating_supply
FROM address_balances
WHERE address != '0x0000000000000000000000000000000000000000'