WITH deduped AS (
    SELECT DISTINCT * FROM "ECOMMERCE"."ECOMMERCE"."REV_TRANSACTIONS"
),
transactions_per_country AS (
    SELECT 
        "channelGrouping",
        "geoNetwork_country",
        COUNT(DISTINCT "hits_transaction_transactionId") AS country_transactions
    FROM deduped
    WHERE "hits_transaction_transactionId" IS NOT NULL
    GROUP BY "channelGrouping", "geoNetwork_country"
),
channel_multiple_countries AS (
    SELECT 
        "channelGrouping",
        COUNT(DISTINCT "geoNetwork_country") AS num_countries
    FROM transactions_per_country
    GROUP BY "channelGrouping"
    HAVING COUNT(DISTINCT "geoNetwork_country") > 1
),
country_ranks AS (
    SELECT 
        tpc."channelGrouping",
        tpc."geoNetwork_country",
        tpc.country_transactions,
        ROW_NUMBER() OVER (PARTITION BY tpc."channelGrouping" ORDER BY tpc.country_transactions DESC) AS rn
    FROM transactions_per_country tpc
    INNER JOIN channel_multiple_countries cmc 
        ON tpc."channelGrouping" = cmc."channelGrouping"
),
channel_totals AS (
    SELECT 
        "channelGrouping",
        SUM(country_transactions) AS total_transactions
    FROM transactions_per_country
    GROUP BY "channelGrouping"
)
SELECT 
    cr."channelGrouping",
    cr."geoNetwork_country" AS country,
    ct.total_transactions AS sum_transactions_for_channel
FROM country_ranks cr
INNER JOIN channel_totals ct ON cr."channelGrouping" = ct."channelGrouping"
WHERE cr.rn = 1
ORDER BY cr."channelGrouping"