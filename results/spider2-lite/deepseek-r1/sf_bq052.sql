WITH patent_cpc AS (
    SELECT DISTINCT "patent_id"
    FROM "PATENTSVIEW"."PATENTSVIEW"."CPC_CURRENT"
    WHERE "subsection_id" = 'C05' OR "group_id" = 'A01G'
),
patent_details AS (
    SELECT 
        p."id" AS "patent_id",
        p."title",
        p."abstract",
        TRY_TO_DATE(a."date", 'YYYY-MM-DD') AS "application_date"
    FROM "PATENTSVIEW"."PATENTSVIEW"."PATENT" p
    INNER JOIN "PATENTSVIEW"."PATENTSVIEW"."APPLICATION" a
        ON p."id" = a."patent_id"
    WHERE p."country" = 'US'
),
backward_citations AS (
    SELECT 
        uc."citation_id" AS "patent_id",
        COUNT(*) AS "backward_count"
    FROM "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" uc
    INNER JOIN patent_details pd
        ON uc."citation_id" = pd."patent_id"
    WHERE 
        TRY_TO_DATE(uc."date", 'YYYY-MM-DD') >= DATEADD(month, -1, pd."application_date")
        AND TRY_TO_DATE(uc."date", 'YYYY-MM-DD') < pd."application_date"
    GROUP BY uc."citation_id"
),
forward_citations AS (
    SELECT 
        uc."citation_id" AS "patent_id",
        COUNT(*) AS "forward_count"
    FROM "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" uc
    INNER JOIN patent_details pd
        ON uc."citation_id" = pd."patent_id"
    WHERE 
        TRY_TO_DATE(uc."date", 'YYYY-MM-DD') > pd."application_date"
        AND TRY_TO_DATE(uc."date", 'YYYY-MM-DD') <= DATEADD(month, 1, pd."application_date")
    GROUP BY uc."citation_id"
)
SELECT 
    pd."patent_id",
    pd."title",
    pd."application_date",
    COALESCE(bc."backward_count", 0) AS "backward_citations",
    COALESCE(fc."forward_count", 0) AS "forward_citations",
    pd."abstract"
FROM patent_details pd
INNER JOIN patent_cpc pc
    ON pd."patent_id" = pc."patent_id"
LEFT JOIN backward_citations bc
    ON pd."patent_id" = bc."patent_id"
LEFT JOIN forward_citations fc
    ON pd."patent_id" = fc."patent_id"
WHERE 
    COALESCE(bc."backward_count", 0) > 0 
    OR COALESCE(fc."forward_count", 0) > 0
ORDER BY pd."application_date"