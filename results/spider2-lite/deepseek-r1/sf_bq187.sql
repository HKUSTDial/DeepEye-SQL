WITH bnb_token AS (
    SELECT "address" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKENS" WHERE "symbol" = 'BNB'
), bnb_transfers AS (
    SELECT "from_address", "to_address", TRY_CAST("value" AS NUMERIC) AS "value_num" FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TOKEN_TRANSFERS" WHERE "token_address" IN (SELECT "address" FROM bnb_token) AND "from_address" != '0x0000000000000000000000000000000000000000' AND "to_address" != '0x0000000000000000000000000000000000000000'
), received AS (
    SELECT "to_address" AS "address", SUM("value_num") AS "total_received" FROM bnb_transfers GROUP BY "to_address"
), sent AS (
    SELECT "from_address" AS "address", SUM("value_num") AS "total_sent" FROM bnb_transfers GROUP BY "from_address"
), balances AS (
    SELECT COALESCE(r."address", s."address") AS "address", COALESCE(r."total_received", 0) - COALESCE(s."total_sent", 0) AS "balance_raw" FROM received r FULL OUTER JOIN sent s ON r."address" = s."address"
)
SELECT SUM("balance_raw") / POWER(10, 18) AS "circulating_supply" FROM balances WHERE "address" != '0x0000000000000000000000000000000000000000'