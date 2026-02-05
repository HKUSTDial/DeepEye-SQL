SELECT
  t."market",
  COUNT(DISTINCT t."season") AS "peak_seasons_count"
FROM
  "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_HISTORICAL_TEAMS_SEASONS" AS t
INNER JOIN (
  SELECT
    "season",
    MAX("wins") AS "max_wins"
  FROM
    "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_HISTORICAL_TEAMS_SEASONS"
  WHERE
    "season" BETWEEN 1900 AND 2000
    AND "market" IS NOT NULL
  GROUP BY
    "season"
) AS max_wins_per_season
ON
  t."season" = max_wins_per_season."season"
  AND t."wins" = max_wins_per_season."max_wins"
WHERE
  t."season" BETWEEN 1900 AND 2000
  AND t."market" IS NOT NULL
GROUP BY
  t."market"
ORDER BY
  "peak_seasons_count" DESC
LIMIT 5