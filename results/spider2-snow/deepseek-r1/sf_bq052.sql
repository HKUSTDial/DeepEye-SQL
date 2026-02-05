WITH patent_data AS (
    SELECT 
        p."id", 
        p."title", 
        TRY_TO_DATE(a."date", 'YYYY-MM-DD') AS app_date,
        p."abstract",
        COUNT(DISTINCT CASE 
            WHEN TRY_TO_DATE(uc."date", 'YYYY-MM-DD') IS NOT NULL 
                AND TRY_TO_DATE(uc."date", 'YYYY-MM-DD') >= DATEADD(month, -1, TRY_TO_DATE(a."date", 'YYYY-MM-DD'))
                AND TRY_TO_DATE(uc."date", 'YYYY-MM-DD') < TRY_TO_DATE(a."date", 'YYYY-MM-DD')
            THEN uc."patent_id" 
        END) AS backward_citations,
        COUNT(DISTINCT CASE 
            WHEN TRY_TO_DATE(uc."date", 'YYYY-MM-DD') IS NOT NULL 
                AND TRY_TO_DATE(uc."date", 'YYYY-MM-DD') > TRY_TO_DATE(a."date", 'YYYY-MM-DD')
                AND TRY_TO_DATE(uc."date", 'YYYY-MM-DD') <= DATEADD(month, 1, TRY_TO_DATE(a."date", 'YYYY-MM-DD'))
            THEN uc."patent_id" 
        END) AS forward_citations
    FROM "PATENTSVIEW"."PATENTSVIEW"."PATENT" p
    JOIN "PATENTSVIEW"."PATENTSVIEW"."APPLICATION" a 
        ON p."id" = a."patent_id"
    LEFT JOIN "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" uc 
        ON uc."citation_id" = p."id"
    WHERE p."country" = 'US'
        AND TRY_TO_DATE(a."date", 'YYYY-MM-DD') IS NOT NULL
        AND EXISTS (
            SELECT 1 
            FROM "PATENTSVIEW"."PATENTSVIEW"."CPC_CURRENT" c
            WHERE c."patent_id" = p."id"
                AND (c."subsection_id" = 'C05' OR c."group_id" = 'A01G')
        )
    GROUP BY p."id", p."title", TRY_TO_DATE(a."date", 'YYYY-MM-DD'), p."abstract"
)
SELECT 
    "id",
    "title",
    app_date AS application_date,
    "abstract",
    backward_citations,
    forward_citations
FROM patent_data
WHERE backward_citations > 0 OR forward_citations > 0
ORDER BY app_date