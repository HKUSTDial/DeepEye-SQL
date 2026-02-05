WITH deduped AS (
  SELECT DISTINCT *
  FROM `data-to-insights.ecommerce.rev_transactions`
),
transactions AS (
  SELECT *
  FROM deduped
  WHERE `totals_transactions` > 0
),
country_agg AS (
  SELECT
    `channelGrouping`,
    `geoNetwork_country` AS country,
    SUM(`totals_transactions`) AS country_transactions
  FROM transactions
  GROUP BY `channelGrouping`, `geoNetwork_country`
),
channel_agg AS (
  SELECT
    `channelGrouping`,
    SUM(country_transactions) AS channel_total_transactions,
    COUNT(DISTINCT country) AS num_countries
  FROM country_agg
  GROUP BY `channelGrouping`
),
multi_country_channels AS (
  SELECT *
  FROM channel_agg
  WHERE num_countries > 1
),
top_country_per_channel AS (
  SELECT
    ca.`channelGrouping`,
    ca.country,
    ca.country_transactions,
    mc.channel_total_transactions,
    ROW_NUMBER() OVER (PARTITION BY ca.`channelGrouping` ORDER BY ca.country_transactions DESC, ca.country) AS rn
  FROM country_agg ca
  INNER JOIN multi_country_channels mc ON ca.`channelGrouping` = mc.`channelGrouping`
)
SELECT
  `channelGrouping`,
  country,
  channel_total_transactions AS sum_of_transactions
FROM top_country_per_channel
WHERE rn = 1
ORDER BY `channelGrouping`