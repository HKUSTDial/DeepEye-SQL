WITH patents_with_a61 AS (
    SELECT DISTINCT p."publication_number"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" AS p
    JOIN LATERAL FLATTEN(INPUT => p."cpc") AS cpc
    WHERE cpc.value LIKE 'A61%'
),
assignee_counts AS (
    SELECT 
        ah.value AS assignee_name,
        COUNT(DISTINCT p."publication_number") AS patent_count
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" AS p
    JOIN LATERAL FLATTEN(INPUT => p."assignee_harmonized") AS ah
    WHERE ah.value IS NOT NULL
        AND EXISTS (
            SELECT 1 
            FROM patents_with_a61 a61 
            WHERE a61."publication_number" = p."publication_number"
        )
    GROUP BY ah.value
    ORDER BY patent_count DESC
    LIMIT 1
),
assignee_patents AS (
    SELECT 
        p."publication_number",
        EXTRACT(YEAR FROM TRY_TO_DATE(CAST(p."filing_date" AS STRING), 'YYYYMMDD')) AS filing_year,
        p."country_code"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" AS p
    JOIN LATERAL FLATTEN(INPUT => p."assignee_harmonized") AS ah
    WHERE ah.value = (SELECT assignee_name FROM assignee_counts)
        AND p."filing_date" IS NOT NULL
        AND ah.value IS NOT NULL
),
busiest_year AS (
    SELECT filing_year
    FROM assignee_patents
    GROUP BY filing_year
    ORDER BY COUNT(DISTINCT "publication_number") DESC
    LIMIT 1
),
jurisdiction_counts AS (
    SELECT 
        ap."country_code",
        COUNT(DISTINCT ap."publication_number") AS patent_count
    FROM assignee_patents ap
    WHERE ap.filing_year = (SELECT filing_year FROM busiest_year)
        AND ap."country_code" IS NOT NULL
    GROUP BY ap."country_code"
    ORDER BY patent_count DESC
    LIMIT 5
)
SELECT LISTAGG("country_code", ',') WITHIN GROUP (ORDER BY patent_count DESC) AS top_jurisdictions
FROM jurisdiction_counts