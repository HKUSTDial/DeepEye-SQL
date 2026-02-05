WITH transaction_details AS (
    SELECT 
        bm."region" AS region,
        TO_DATE(bt."txn_date", 'DD-MM-YYYY') AS txn_date,
        EXTRACT(YEAR FROM TO_DATE(bt."txn_date", 'DD-MM-YYYY')) AS year,
        bt."quantity" AS quantity,
        bp."price" AS price,
        bt."quantity" * bp."price" AS dollar_amount
    FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."BITCOIN_TRANSACTIONS" bt
    INNER JOIN "BANK_SALES_TRADING"."BANK_SALES_TRADING"."BITCOIN_MEMBERS" bm 
        ON bt."member_id" = bm."member_id"
    INNER JOIN "BANK_SALES_TRADING"."BANK_SALES_TRADING"."BITCOIN_PRICES" bp 
        ON bt."ticker" = bp."ticker" 
        AND bt."txn_date" = bp."market_date"
    WHERE bt."txn_type" = 'BUY'
        AND bt."ticker" = 'BTC'
),
region_year_totals AS (
    SELECT 
        region,
        year,
        SUM(dollar_amount) AS total_dollar_amount,
        SUM(quantity) AS total_quantity,
        SUM(dollar_amount) / SUM(quantity) AS avg_price
    FROM transaction_details
    GROUP BY region, year
),
first_year_per_region AS (
    SELECT region, MIN(year) AS first_year
    FROM region_year_totals
    GROUP BY region
),
region_year_filtered AS (
    SELECT ryt.*
    FROM region_year_totals ryt
    LEFT JOIN first_year_per_region fyr ON ryt.region = fyr.region
    WHERE ryt.year != fyr.first_year
),
with_rank AS (
    SELECT 
        region,
        year,
        avg_price,
        RANK() OVER (PARTITION BY year ORDER BY avg_price DESC) AS rank
    FROM region_year_filtered
)
SELECT 
    region,
    year,
    avg_price,
    rank,
    (avg_price - LAG(avg_price) OVER (PARTITION BY region ORDER BY year)) / LAG(avg_price) OVER (PARTITION BY region ORDER BY year) * 100 AS percentage_change
FROM with_rank
ORDER BY year, rank