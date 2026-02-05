SELECT p."id", p."number", app."date", COUNT(DISTINCT bc."citation_id") AS backward_citations_count, COUNT(DISTINCT fc."patent_id") AS forward_citations_count
FROM "PATENTSVIEW"."PATENTSVIEW"."PATENT" p
INNER JOIN "PATENTSVIEW"."PATENTSVIEW"."APPLICATION" app ON p."id" = app."patent_id"
INNER JOIN "PATENTSVIEW"."PATENTSVIEW"."CPC_CURRENT" cpc ON p."id" = cpc."patent_id"
LEFT JOIN "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" bc ON p."id" = bc."patent_id" AND TRY_TO_DATE(bc."date") BETWEEN DATEADD(year, -1, TRY_TO_DATE(app."date")) AND TRY_TO_DATE(app."date")
LEFT JOIN "PATENTSVIEW"."PATENTSVIEW"."USPATENTCITATION" fc ON p."id" = fc."citation_id" AND TRY_TO_DATE(fc."date") BETWEEN TRY_TO_DATE(app."date") AND DATEADD(year, 3, TRY_TO_DATE(app."date"))
WHERE p."country" = 'US' AND cpc."category" = 'inventional' AND cpc."section_id" = 'C'
GROUP BY p."id", p."number", app."date"
HAVING COUNT(DISTINCT bc."citation_id") > 0 AND COUNT(DISTINCT CASE WHEN TRY_TO_DATE(fc."date") BETWEEN TRY_TO_DATE(app."date") AND DATEADD(year, 1, TRY_TO_DATE(app."date")) THEN fc."patent_id" END) > 0
ORDER BY backward_citations_count DESC
LIMIT 1