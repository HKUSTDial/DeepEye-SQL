WITH RECURSIVE patent_data AS (
  SELECT 
    "cpc"[0]::STRING AS first_cpc,
    CAST(SUBSTRING(CAST("filing_date" AS VARCHAR), 1, 4) AS INT) AS filing_year
  FROM "PATENTS"."PATENTS"."PUBLICATIONS"
  WHERE "filing_date" IS NOT NULL
    AND "application_number" IS NOT NULL AND "application_number" != ''
    AND "cpc" IS NOT NULL AND ARRAY_SIZE("cpc") > 0 AND "cpc"[0] IS NOT NULL
),
cpc_hierarchy AS (
  SELECT 
    pd.first_cpc,
    pd.filing_year,
    cd."symbol",
    cd."level",
    cd."parents"
  FROM patent_data pd
  JOIN "PATENTS"."PATENTS"."CPC_DEFINITION" cd
    ON cd."symbol" = pd.first_cpc
),
ancestors AS (
  SELECT 
    ch.first_cpc,
    ch.filing_year,
    parent_arr.value::STRING AS ancestor_symbol
  FROM cpc_hierarchy ch,
    LATERAL FLATTEN(INPUT => ch."parents") parent_arr
  UNION ALL
  SELECT 
    ch.first_cpc,
    ch.filing_year,
    ch."symbol" AS ancestor_symbol
  FROM cpc_hierarchy ch
  WHERE ch."level" = 5
),
ancestor_levels AS (
  SELECT 
    a.first_cpc,
    a.filing_year,
    a.ancestor_symbol,
    cd."level"
  FROM ancestors a
  JOIN "PATENTS"."PATENTS"."CPC_DEFINITION" cd
    ON cd."symbol" = a.ancestor_symbol
),
level5_ancestors AS (
  SELECT DISTINCT
    first_cpc,
    filing_year,
    ancestor_symbol AS level5_symbol
  FROM ancestor_levels
  WHERE "level" = 5
),
yearly_counts AS (
  SELECT 
    level5_symbol,
    filing_year,
    COUNT(*) AS patent_count
  FROM level5_ancestors
  GROUP BY level5_symbol, filing_year
),
yearly_counts_with_rownum AS (
  SELECT 
    level5_symbol,
    filing_year,
    patent_count,
    ROW_NUMBER() OVER (PARTITION BY level5_symbol ORDER BY filing_year) AS rn
  FROM yearly_counts
),
ema_calc AS (
  SELECT 
    level5_symbol,
    filing_year,
    patent_count,
    rn,
    patent_count AS ema
  FROM yearly_counts_with_rownum
  WHERE rn = 1
  UNION ALL
  SELECT 
    y.level5_symbol,
    y.filing_year,
    y.patent_count,
    y.rn,
    0.2 * y.patent_count + 0.8 * e.ema AS ema
  FROM yearly_counts_with_rownum y
  JOIN ema_calc e
    ON y.level5_symbol = e.level5_symbol
    AND y.rn = e.rn + 1
),
ranked_ema AS (
  SELECT 
    level5_symbol,
    filing_year,
    ema,
    ROW_NUMBER() OVER (PARTITION BY level5_symbol ORDER BY ema DESC, filing_year ASC) AS rn
  FROM ema_calc
)
SELECT 
  cd."titleFull" AS cpc_title,
  re.filing_year AS best_year,
  re.ema AS highest_exponential_moving_average
FROM ranked_ema re
JOIN "PATENTS"."PATENTS"."CPC_DEFINITION" cd
  ON cd."symbol" = re.level5_symbol
WHERE re.rn = 1
ORDER BY cd."titleFull"