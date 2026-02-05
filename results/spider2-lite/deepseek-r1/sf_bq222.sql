WITH german_dec2016_patents AS (
    SELECT 
        "publication_number",
        "filing_date",
        FLATTENED_CPC.value::STRING AS cpc_code
    FROM "PATENTS"."PATENTS"."PUBLICATIONS"
    JOIN LATERAL FLATTEN(INPUT => "cpc") AS FLATTENED_CPC
    WHERE "country_code" = 'DE'
      AND "grant_date" BETWEEN 20161201 AND 20161231
),
cpc_level4 AS (
    SELECT 
        "symbol",
        "titleFull"
    FROM "PATENTS"."PATENTS"."CPC_DEFINITION"
    WHERE "level" = 4
),
annual_counts AS (
    SELECT 
        c."symbol",
        c."titleFull",
        FLOOR(p."filing_date" / 10000) AS filing_year,
        COUNT(*) AS filings
    FROM german_dec2016_patents p
    JOIN cpc_level4 c ON p.cpc_code = c."symbol"
    GROUP BY c."symbol", c."titleFull", FLOOR(p."filing_date" / 10000)
),
ordered_counts AS (
    SELECT 
        "symbol",
        "titleFull",
        filing_year,
        filings,
        ROW_NUMBER() OVER (PARTITION BY "symbol" ORDER BY filing_year) AS rn
    FROM annual_counts
),
recursive_ema AS (
    SELECT 
        "symbol",
        "titleFull",
        filing_year,
        filings,
        filings AS ema,
        rn
    FROM ordered_counts
    WHERE rn = 1
    
    UNION ALL
    
    SELECT 
        oc."symbol",
        oc."titleFull",
        oc.filing_year,
        oc.filings,
        0.1 * oc.filings + 0.9 * re.ema AS ema,
        oc.rn
    FROM ordered_counts oc
    JOIN recursive_ema re ON oc."symbol" = re."symbol" AND oc.rn = re.rn + 1
),
ranked_ema AS (
    SELECT 
        "symbol",
        "titleFull",
        filing_year,
        ema,
        ROW_NUMBER() OVER (PARTITION BY "symbol" ORDER BY ema DESC) AS ema_rank
    FROM recursive_ema
)
SELECT 
    "titleFull",
    "symbol" AS cpc_group,
    filing_year AS best_year
FROM ranked_ema
WHERE ema_rank = 1
ORDER BY "titleFull", "symbol"