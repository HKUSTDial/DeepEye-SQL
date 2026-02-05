WITH hourly_aggregates AS (
    SELECT 
        EXTRACT(YEAR FROM TO_TIMESTAMP("block_timestamp" / 1000000)) as year,
        EXTRACT(HOUR FROM TO_TIMESTAMP("block_timestamp" / 1000000)) as hour_of_day,
        DATE_TRUNC('hour', TO_TIMESTAMP("block_timestamp" / 1000000)) as hour_start,
        SUM(TRY_TO_NUMERIC("value")) as hourly_total
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TOKEN_TRANSFERS"
    WHERE "token_address" = '0x68e54af74b22acaccffa04ccaad13be16ed14eac'
        AND ("from_address" = '0x8babf0ba311aab914c00e8fda7e8558a8b66de5d' 
             OR "to_address" = '0xfbd6c6b112214d949dcdfb1217153bc0a742862f')
        AND "block_timestamp" >= 1546300800000000
        AND "block_timestamp" < 1609459200000000
    GROUP BY year, hour_of_day, hour_start
),
hourly_changes AS (
    SELECT 
        year,
        hour_start,
        hourly_total,
        LAG(hourly_total) OVER (PARTITION BY year ORDER BY hour_start) as prev_hour_total,
        hourly_total - LAG(hourly_total) OVER (PARTITION BY year ORDER BY hour_start) as hourly_change
    FROM hourly_aggregates
),
yearly_avg_changes AS (
    SELECT 
        year,
        AVG(hourly_change) as avg_hourly_change
    FROM hourly_changes
    WHERE prev_hour_total IS NOT NULL
    GROUP BY year
)
SELECT 
    MAX(CASE WHEN year = 2019 THEN avg_hourly_change ELSE 0 END) -
    MAX(CASE WHEN year = 2020 THEN avg_hourly_change ELSE 0 END) as difference
FROM yearly_avg_changes