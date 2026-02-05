WITH counts AS (
    SELECT 
        "STATE_ABBREVIATION" as state,
        COUNT(CASE WHEN "START_DATE" <= '2020-03-01' AND ("END_DATE" IS NULL OR "END_DATE" >= '2020-03-01') THEN 1 END) as count_20200301,
        COUNT(CASE WHEN "START_DATE" <= '2021-12-31' AND ("END_DATE" IS NULL OR "END_DATE" >= '2021-12-31') THEN 1 END) as count_20211231
    FROM 
        "FINANCE__ECONOMICS"."CYBERSYN"."FINANCIAL_BRANCH_ENTITIES"
    WHERE 
        "ID_COUNTRY" = 'country/USA' 
        AND "CATEGORY" = 'Branch'
    GROUP BY 
        "STATE_ABBREVIATION"
)
SELECT 
    state,
    count_20200301,
    count_20211231,
    CASE 
        WHEN count_20200301 > 0 THEN 
            (count_20211231 - count_20200301) * 100.0 / count_20200301 
        ELSE NULL 
    END as pct_change
FROM counts
ORDER BY state