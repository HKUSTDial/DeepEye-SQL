SELECT
  p."id",
  p."title",
  p."abstract",
  p."date" AS publication_date,
  COALESCE((
    SELECT COUNT(DISTINCT u."citation_id")
    FROM "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" u
    WHERE u."patent_id" = p."id"
      AND TRY_TO_DATE(u."date", 'YYYY-MM-DD') < TRY_TO_DATE(a."date", 'YYYY-MM-DD')
  ), 0) + COALESCE((
    SELECT COUNT(DISTINCT f."number")
    FROM "PATENTSVIEW"."PATENTSVIEW"."FOREIGNCITATION" f
    WHERE f."patent_id" = p."id"
      AND TRY_TO_DATE(f."date", 'YYYY-MM-DD') < TRY_TO_DATE(a."date", 'YYYY-MM-DD')
  ), 0) AS backward_citations,
  COALESCE((
    SELECT COUNT(DISTINCT u2."patent_id")
    FROM "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" u2
    WHERE u2."citation_id" = p."id"
      AND TRY_TO_DATE(u2."date", 'YYYY-MM-DD') BETWEEN TRY_TO_DATE(p."date", 'YYYY-MM-DD') AND DATEADD(year, 5, TRY_TO_DATE(p."date", 'YYYY-MM-DD'))
  ), 0) AS forward_citations
FROM "PATENTSVIEW"."PATENTSVIEW"."PATENT" p
INNER JOIN "PATENTSVIEW"."PATENTSVIEW"."APPLICATION" a ON p."id" = a."patent_id"
WHERE p."country" = 'US'
  AND TRY_TO_DATE(a."date", 'YYYY-MM-DD') BETWEEN '2014-01-01' AND '2014-02-01'
  AND EXISTS (
    SELECT 1
    FROM "PATENTSVIEW"."PATENTSVIEW"."CPC_CURRENT" c
    WHERE c."patent_id" = p."id"
      AND (TRY_CAST(SUBSTR(c."subsection_id", 2) AS INTEGER) BETWEEN 5 AND 13
           OR c."group_id" IN ('A01G','A01H','A61K','A61P','A61Q','B01F','B01J','B81B','B82B','B82Y','G01N','G16H'))
  )