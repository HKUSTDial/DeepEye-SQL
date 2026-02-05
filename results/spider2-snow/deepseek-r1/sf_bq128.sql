WITH eligible_patents AS (
    SELECT DISTINCT
        p."id" AS patent_id,
        p."title",
        p."abstract",
        TRY_TO_DATE(p."date", 'YYYY-MM-DD') AS publication_date,
        TRY_TO_DATE(a."date", 'YYYY-MM-DD') AS filing_date
    FROM "PATENTSVIEW"."PATENTSVIEW"."PATENT" p
    INNER JOIN "PATENTSVIEW"."PATENTSVIEW"."APPLICATION" a
        ON p."id" = a."patent_id"
    INNER JOIN "PATENTSVIEW"."PATENTSVIEW"."CPC_CURRENT" c
        ON p."id" = c."patent_id"
    WHERE p."country" = 'US'
        AND a."country" = 'US'
        AND TRY_TO_DATE(a."date", 'YYYY-MM-DD') BETWEEN TO_DATE('2014-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-02-01', 'YYYY-MM-DD')
        AND (c."subsection_id" BETWEEN 'C05' AND 'C13'
             OR c."group_id" IN ('A01G','A01H','A61K','A61P','A61Q','B01F','B01J','B81B','B82B','B82Y','G01N','G16H'))
        AND TRY_TO_DATE(p."date", 'YYYY-MM-DD') IS NOT NULL
)
SELECT
    ep."title",
    ep."abstract",
    ep.publication_date,
    (SELECT COUNT(*)
     FROM "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" usc
     WHERE usc."patent_id" = ep.patent_id
       AND TRY_TO_DATE(usc."date", 'YYYY-MM-DD') IS NOT NULL
       AND TRY_TO_DATE(usc."date", 'YYYY-MM-DD') < ep.filing_date
    ) +
    (SELECT COUNT(*)
     FROM "PATENTSVIEW"."PATENTSVIEW"."FOREIGNCITATION" fc
     WHERE fc."patent_id" = ep.patent_id
       AND TRY_TO_DATE(fc."date", 'YYYY-MM-DD') IS NOT NULL
       AND TRY_TO_DATE(fc."date", 'YYYY-MM-DD') < ep.filing_date
    ) AS backward_citation_count,
    (SELECT COUNT(*)
     FROM "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" usc2
     WHERE usc2."citation_id" = ep.patent_id
       AND TRY_TO_DATE(usc2."date", 'YYYY-MM-DD') IS NOT NULL
       AND TRY_TO_DATE(usc2."date", 'YYYY-MM-DD') BETWEEN ep.publication_date AND DATEADD(year, 5, ep.publication_date)
    ) AS forward_citation_count
FROM eligible_patents ep
ORDER BY ep.patent_id