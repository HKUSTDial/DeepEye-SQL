SELECT "address", SUM("amount") AS "net_balance"
FROM (
    SELECT "to_address" AS "address", TRY_TO_DECIMAL("value", 38, 0) AS "amount"
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TOKEN_TRANSFERS"
    WHERE "token_address" = '0xa92a861fc11b99b24296af880011b47f9cafb5ab'
    UNION ALL
    SELECT "from_address" AS "address", -TRY_TO_DECIMAL("value", 38, 0) AS "amount"
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TOKEN_TRANSFERS"
    WHERE "token_address" = '0xa92a861fc11b99b24296af880011b47f9cafb5ab'
) t
GROUP BY "address"
HAVING SUM("amount") > 0
ORDER BY "net_balance" ASC
LIMIT 3