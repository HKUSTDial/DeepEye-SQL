WITH duplicate_check AS (
    SELECT 
        "hash",
        TRUE AS has_duplicate_output_values
    FROM (
        SELECT 
            "hash",
            VALUE:"value" AS output_value_single
        FROM 
            "CRYPTO"."CRYPTO_BITCOIN"."TRANSACTIONS",
            LATERAL FLATTEN(INPUT => "outputs")
        WHERE 
            "block_timestamp_month" >= '2023-07-01'
        GROUP BY 
            "hash", output_value_single
        HAVING 
            COUNT(*) >= 2
    )
    GROUP BY "hash"
),
transactions_with_flag AS (
    SELECT 
        t.*,
        COALESCE(d.has_duplicate_output_values, FALSE) AS has_duplicate_output_values
    FROM 
        "CRYPTO"."CRYPTO_BITCOIN"."TRANSACTIONS" t
        LEFT JOIN duplicate_check d ON t."hash" = d."hash"
    WHERE 
        t."block_timestamp_month" >= '2023-07-01'
),
monthly_aggregates AS (
    SELECT 
        "block_timestamp_month" AS month,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN "output_count" > 2 AND "output_value" <= "input_value" AND has_duplicate_output_values = TRUE THEN 1 ELSE 0 END) AS coinjoin_transactions,
        SUM("input_count" + "output_count") AS total_utxos,
        SUM(CASE WHEN "output_count" > 2 AND "output_value" <= "input_value" AND has_duplicate_output_values = TRUE THEN ("input_count" + "output_count") ELSE 0 END) AS coinjoin_utxos,
        SUM("input_value") AS total_volume,
        SUM(CASE WHEN "output_count" > 2 AND "output_value" <= "input_value" AND has_duplicate_output_values = TRUE THEN "input_value" ELSE 0 END) AS coinjoin_volume
    FROM 
        transactions_with_flag
    GROUP BY 
        "block_timestamp_month"
)
SELECT 
    month,
    (coinjoin_transactions * 100.0 / total_transactions) AS transaction_percentage,
    (coinjoin_utxos * 100.0 / total_utxos) AS utxo_percentage,
    (coinjoin_volume * 100.0 / total_volume) AS volume_percentage
FROM 
    monthly_aggregates
ORDER BY 
    month