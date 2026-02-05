WITH focal_patents AS (
    SELECT 
        "publication_number",
        "grant_date",
        "citation"
    FROM 
        "PATENTS"."PATENTS"."PUBLICATIONS"
    WHERE 
        "country_code" = 'US'
        AND "kind_code" = 'B2'
        AND "grant_date" BETWEEN 20150101 AND 20181231
),
citations AS (
    SELECT 
        fp."publication_number" AS focal_patent,
        c.value:"publication_number"::TEXT AS cited_patent
    FROM 
        focal_patents fp,
        LATERAL FLATTEN(INPUT => fp."citation") c
),
cited_ipc_counts AS (
    SELECT 
        p."publication_number",
        SUBSTR(ipc.value:"code"::TEXT, 1, 4) AS ipc4,
        COUNT(*) AS ipc4_count
    FROM 
        "PATENTS"."PATENTS"."PUBLICATIONS" p,
        LATERAL FLATTEN(INPUT => p."ipc") ipc
    WHERE 
        p."publication_number" IN (SELECT cited_patent FROM citations)
    GROUP BY 
        p."publication_number",
        ipc4
),
ranked_cited_ipc AS (
    SELECT 
        "publication_number",
        ipc4,
        ROW_NUMBER() OVER (PARTITION BY "publication_number" ORDER BY ipc4_count DESC) AS rn
    FROM 
        cited_ipc_counts
),
cited_ipc_main AS (
    SELECT 
        "publication_number",
        ipc4
    FROM 
        ranked_cited_ipc
    WHERE 
        rn = 1
),
citation_ipc AS (
    SELECT 
        c.focal_patent,
        ci.ipc4
    FROM 
        citations c
        JOIN cited_ipc_main ci ON c.cited_patent = ci."publication_number"
),
ipc_counts_per_focal AS (
    SELECT 
        focal_patent,
        ipc4,
        COUNT(*) AS ipc_count
    FROM 
        citation_ipc
    GROUP BY 
        focal_patent,
        ipc4
),
total_citations_per_focal AS (
    SELECT 
        focal_patent,
        COUNT(*) AS total_citations
    FROM 
        citation_ipc
    GROUP BY 
        focal_patent
),
originality_scores AS (
    SELECT 
        icpf.focal_patent,
        1.0 - SUM(POWER(icpf.ipc_count * 1.0 / tcpf.total_citations, 2)) AS originality_score
    FROM 
        ipc_counts_per_focal icpf
        JOIN total_citations_per_focal tcpf ON icpf.focal_patent = tcpf.focal_patent
    GROUP BY 
        icpf.focal_patent
)
SELECT 
    focal_patent AS publication_number,
    originality_score
FROM 
    originality_scores
WHERE 
    originality_score IS NOT NULL
ORDER BY 
    originality_score DESC
LIMIT 1