SELECT
    TO_DATE(TO_TIMESTAMP("block_timestamp" / 1000000)) AS "date",
    TO_CHAR(
        SUM(
            CASE 
                WHEN "input" LIKE '0x40c10f19%' THEN 1 
                ELSE -1 
            END *
            COALESCE(
                TO_NUMBER(
                    LTRIM(
                        SUBSTR("input", 
                            CASE 
                                WHEN "input" LIKE '0x40c10f19%' THEN 75 
                                ELSE 11 
                            END, 
                            64
                        ),
                        '0'
                    ),
                    'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
                ),
                0
            ) / 1000000.0
        ),
        '$999,999,999,999,999,999.99'
    ) AS "Δ Total Market Value"
FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRANSACTIONS"
WHERE "to_address" = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    AND ("input" LIKE '0x40c10f19%' OR "input" LIKE '0x42966c68%')
    AND "block_timestamp" >= 1672531200000000
    AND "block_timestamp" < 1704067200000000
GROUP BY "date"
ORDER BY "date" DESC