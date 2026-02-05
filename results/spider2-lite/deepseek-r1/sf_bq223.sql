WITH DENSO_PATENTS AS (
    SELECT DISTINCT p."publication_number"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" p,
    LATERAL FLATTEN(input => p."assignee") a
    WHERE a.value::string = 'DENSO CORP'
),
CITATIONS_TO_DENSO AS (
    SELECT 
        citing."publication_number" as citing_pub,
        citing."assignee" as citing_assignee_array,
        citing."cpc" as citing_cpc_array,
        citing."filing_date" as "filing_date"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" citing,
    LATERAL FLATTEN(input => citing."citation") cited
    WHERE cited.value::string IN (SELECT "publication_number" FROM DENSO_PATENTS)
        AND citing."filing_date" IS NOT NULL
        AND citing."filing_date" > 0
),
CITING_PATENTS_DETAILS AS (
    SELECT 
        citing_pub,
        assignee.value::string as citing_assignee,
        citing_cpc_array[0]::string as first_cpc_code,
        "filing_date"
    FROM CITATIONS_TO_DENSO,
    LATERAL FLATTEN(input => citing_assignee_array) assignee
    WHERE assignee.value::string != 'DENSO CORP'
        AND citing_cpc_array[0]::string IS NOT NULL
)
SELECT 
    cpd.citing_assignee,
    cpc."titleFull" as cpc_subclass_title,
    COUNT(*) as citation_count
FROM CITING_PATENTS_DETAILS cpd
LEFT JOIN "PATENTS"."PATENTS"."CPC_DEFINITION" cpc
    ON cpd.first_cpc_code = cpc."symbol"
GROUP BY cpd.citing_assignee, cpc."titleFull"
ORDER BY citation_count DESC