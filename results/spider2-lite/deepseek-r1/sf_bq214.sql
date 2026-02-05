WITH utility_b2_patents AS (
  SELECT 
    p."publication_number",
    p."filing_date",
    EXTRACT(YEAR FROM TO_DATE(CAST(p."filing_date" AS VARCHAR), 'YYYYMMDD')) AS filing_year
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
  WHERE p."country_code" = 'US'
    AND p."application_kind" = 'U'
    AND p."kind_code" = 'B2'
    AND p."grant_date" BETWEEN 20100101 AND 20141231
),
forward_citations AS (
  SELECT 
    u."publication_number",
    u."filing_date",
    u.filing_year,
    COUNT(DISTINCT 
      CASE WHEN p2."publication_number" IS NOT NULL 
           AND DATEDIFF(day, 
                        TO_DATE(CAST(u."filing_date" AS VARCHAR), 'YYYYMMDD'),
                        TO_DATE(CAST(p2."filing_date" AS VARCHAR), 'YYYYMMDD')
                       ) BETWEEN 0 AND 30
           THEN cited.value::STRING 
      END
    ) AS forward_citation_count
  FROM utility_b2_patents u
  LEFT JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" a
    ON u."publication_number" = a."publication_number"
  LEFT JOIN LATERAL FLATTEN(INPUT => a."cited_by") AS cited
  LEFT JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p2
    ON cited.value::STRING = p2."publication_number"
  GROUP BY u."publication_number", u."filing_date", u.filing_year
),
top_patent AS (
  SELECT 
    "publication_number",
    "filing_date",
    filing_year,
    forward_citation_count
  FROM forward_citations
  ORDER BY forward_citation_count DESC
  LIMIT 1
),
similar_patents AS (
  SELECT 
    t."publication_number" AS focal_pub_num,
    t.filing_year,
    sim.value:"publication_number"::STRING AS similar_pub_num,
    sim.value:"score"::FLOAT AS similarity_score
  FROM top_patent t
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" a
    ON t."publication_number" = a."publication_number"
  , LATERAL FLATTEN(INPUT => a."similar") AS sim
  WHERE sim.value:"publication_number"::STRING != t."publication_number"
),
similar_patents_with_year AS (
  SELECT 
    s.focal_pub_num,
    s.similar_pub_num,
    s.similarity_score,
    EXTRACT(YEAR FROM TO_DATE(CAST(p."filing_date" AS VARCHAR), 'YYYYMMDD')) AS similar_filing_year
  FROM similar_patents s
  JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
    ON s.similar_pub_num = p."publication_number"
  WHERE p."filing_date" IS NOT NULL
    AND EXTRACT(YEAR FROM TO_DATE(CAST(p."filing_date" AS VARCHAR), 'YYYYMMDD')) = s.filing_year
)
SELECT 
  focal_pub_num AS patent_with_most_forward_citations,
  similar_pub_num AS most_similar_patent,
  similarity_score
FROM similar_patents_with_year
ORDER BY similarity_score DESC
LIMIT 1