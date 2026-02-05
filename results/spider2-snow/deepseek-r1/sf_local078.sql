WITH max_comp AS (
    SELECT "interest_id", MAX("composition") as max_composition
    FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."INTEREST_METRICS"
    GROUP BY "interest_id"
),
ranked_months AS (
    SELECT m."interest_id", m."month_year", m."composition",
           ROW_NUMBER() OVER (PARTITION BY m."interest_id" ORDER BY m."_year" DESC, m."_month" DESC) as rn
    FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."INTEREST_METRICS" m
    JOIN max_comp mc ON m."interest_id" = mc."interest_id" AND m."composition" = mc.max_composition
),
max_info AS (
    SELECT "interest_id", "month_year", "composition"
    FROM ranked_months
    WHERE rn = 1
),
interest_data AS (
    SELECT mi."interest_id", mi."month_year", mi."composition", im."interest_name"
    FROM max_info mi
    JOIN "BANK_SALES_TRADING"."BANK_SALES_TRADING"."INTEREST_MAP" im
    ON mi."interest_id" = im."id"
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY "composition" DESC) as rank_desc,
           ROW_NUMBER() OVER (ORDER BY "composition" ASC) as rank_asc
    FROM interest_data
)
SELECT "month_year", "interest_name", "composition"
FROM ranked
WHERE rank_desc <= 10 OR rank_asc <= 10
ORDER BY CASE WHEN rank_desc <= 10 THEN rank_desc ELSE rank_asc + 1000 END