SELECT
  p."publication_number",
  COUNT(CASE WHEN f.value:category::VARCHAR = 'SEA' THEN 1 END) AS sea_citation_count
FROM "PATENTS"."PATENTS"."PUBLICATIONS" p
LEFT JOIN LATERAL FLATTEN(INPUT => p."citation") f
WHERE p."grant_date" >= 20100101 AND p."grant_date" <= 20181231
GROUP BY p."publication_number"