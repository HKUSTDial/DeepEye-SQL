WITH eligible_patents AS (
    SELECT 
        p."publication_number",
        p."filing_date",
        e."cited_by"
    FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
    INNER JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" e
        ON p."publication_number" = e."publication_number"
    WHERE p."country_code" = 'US'
        AND p."kind_code" = 'B2'
        AND p."grant_date" >= 20100101
        AND p."grant_date" <= 20141231
),
citation_counts AS (
    SELECT 
        ep."publication_number",
        COUNT(DISTINCT citing."publication_number") AS "citation_count"
    FROM eligible_patents ep
    LEFT JOIN LATERAL FLATTEN(INPUT => ep."cited_by") AS cited_by_list
    LEFT JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" citing
        ON cited_by_list.VALUE::TEXT = citing."publication_number"
        AND ABS(DATEDIFF(day, 
            TO_DATE(CAST(ep."filing_date" AS VARCHAR), 'YYYYMMDD'),
            TO_DATE(CAST(citing."filing_date" AS VARCHAR), 'YYYYMMDD')
        )) <= 30
    GROUP BY ep."publication_number"
),
top_patent AS (
    SELECT 
        ep."publication_number",
        ep."filing_date",
        COALESCE(cc."citation_count", 0) AS "citation_count"
    FROM eligible_patents ep
    LEFT JOIN citation_counts cc ON ep."publication_number" = cc."publication_number"
    ORDER BY COALESCE(cc."citation_count", 0) DESC
    LIMIT 1
),
similar_patents AS (
    SELECT 
        tp."publication_number" AS focal_patent,
        tp."filing_date",
        similar_info.VALUE:"publication_number"::TEXT AS similar_pub_num,
        similar_info.VALUE:"score"::FLOAT AS similarity_score
    FROM top_patent tp
    INNER JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" e
        ON tp."publication_number" = e."publication_number"
    LEFT JOIN LATERAL FLATTEN(INPUT => e."similar") AS similar_info
),
same_year_similar AS (
    SELECT 
        sp.*,
        p."filing_date" AS similar_filing_date
    FROM similar_patents sp
    INNER JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" p
        ON sp.similar_pub_num = p."publication_number"
    WHERE FLOOR(p."filing_date" / 10000) = FLOOR(sp."filing_date" / 10000)
),
most_similar AS (
    SELECT 
        focal_patent,
        similar_pub_num AS most_similar_patent,
        similarity_score
    FROM same_year_similar
    ORDER BY similarity_score DESC
    LIMIT 1
)
SELECT 
    tp."publication_number" AS patent_with_most_citations,
    tp."citation_count",
    ms.most_similar_patent,
    ms.similarity_score
FROM top_patent tp
LEFT JOIN most_similar ms ON tp."publication_number" = ms.focal_patent