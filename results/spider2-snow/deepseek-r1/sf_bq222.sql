WITH RECURSIVE dec_2016_cpc_groups AS (
  SELECT DISTINCT
    "CPC_DEFINITION"."symbol" AS level4_symbol
  FROM
    "PATENTS"."PATENTS"."PUBLICATIONS"
  JOIN LATERAL FLATTEN(INPUT => "PUBLICATIONS"."cpc") AS cpc_list
  INNER JOIN "PATENTS"."PATENTS"."CPC_DEFINITION"
    ON cpc_list.VALUE::STRING LIKE "CPC_DEFINITION"."symbol" || '%'
  WHERE
    "PUBLICATIONS"."country_code" = 'DE'
    AND "PUBLICATIONS"."grant_date" BETWEEN 20161201 AND 20161231
    AND "CPC_DEFINITION"."level" = 4
),
annual_patent_counts AS (
  SELECT
    FLOOR("PUBLICATIONS"."filing_date" / 10000) AS filing_year,
    "CPC_DEFINITION"."symbol" AS level4_symbol,
    COUNT(DISTINCT "PUBLICATIONS"."publication_number") AS patent_count
  FROM
    "PATENTS"."PATENTS"."PUBLICATIONS"
  JOIN LATERAL FLATTEN(INPUT => "PUBLICATIONS"."cpc") AS cpc_list
  INNER JOIN "PATENTS"."PATENTS"."CPC_DEFINITION"
    ON cpc_list.VALUE::STRING LIKE "CPC_DEFINITION"."symbol" || '%'
  WHERE
    "PUBLICATIONS"."country_code" = 'DE'
    AND "PUBLICATIONS"."filing_date" IS NOT NULL
    AND "CPC_DEFINITION"."level" = 4
    AND "CPC_DEFINITION"."symbol" IN (SELECT level4_symbol FROM dec_2016_cpc_groups)
  GROUP BY
    filing_year,
    level4_symbol
),
annual_ordered AS (
  SELECT
    filing_year,
    level4_symbol,
    patent_count,
    ROW_NUMBER() OVER (PARTITION BY level4_symbol ORDER BY filing_year) AS rn
  FROM annual_patent_counts
),
recursive_ema AS (
  SELECT
    filing_year,
    level4_symbol,
    patent_count,
    patent_count AS ema,
    rn
  FROM annual_ordered
  WHERE rn = 1
  UNION ALL
  SELECT
    ao.filing_year,
    ao.level4_symbol,
    ao.patent_count,
    0.1 * ao.patent_count + 0.9 * re.ema AS ema,
    ao.rn
  FROM annual_ordered ao
  INNER JOIN recursive_ema re
    ON ao.level4_symbol = re.level4_symbol
    AND ao.rn = re.rn + 1
),
ranked_ema AS (
  SELECT
    filing_year,
    level4_symbol,
    ema,
    ROW_NUMBER() OVER (PARTITION BY level4_symbol ORDER BY ema DESC, filing_year DESC) AS rn
  FROM recursive_ema
)
SELECT
  "CPC_DEFINITION"."titleFull" AS full_title,
  "CPC_DEFINITION"."symbol" AS cpc_group,
  ranked_ema.filing_year AS best_year
FROM
  ranked_ema
INNER JOIN "PATENTS"."PATENTS"."CPC_DEFINITION"
  ON ranked_ema.level4_symbol = "CPC_DEFINITION"."symbol"
WHERE
  ranked_ema.rn = 1
ORDER BY
  cpc_group