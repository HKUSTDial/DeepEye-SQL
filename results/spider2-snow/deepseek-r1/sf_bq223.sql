WITH denso_patents AS (
  SELECT p."publication_number"
  FROM "PATENTS"."PATENTS"."PUBLICATIONS" p,
  LATERAL FLATTEN(INPUT => p."assignee_harmonized") a
  WHERE a.value:"name"::STRING LIKE '%DENSO CORP%'
    AND p."filing_date" IS NOT NULL
    AND p."filing_date" > 0
),
citations_to_denso AS (
  SELECT 
    citing."publication_number" AS citing_pub,
    cit.value:"publication_number"::STRING AS cited_pub
  FROM "PATENTS"."PATENTS"."PUBLICATIONS" citing,
  LATERAL FLATTEN(INPUT => citing."citation") cit
  WHERE citing."filing_date" IS NOT NULL 
    AND citing."filing_date" > 0
    AND EXISTS (
      SELECT 1 FROM denso_patents dp 
      WHERE dp."publication_number" = cit.value:"publication_number"::STRING
    )
),
citing_with_cpc AS (
  SELECT 
    cd.citing_pub,
    cpc.value:"code"::STRING AS first_cpc_code
  FROM citations_to_denso cd
  INNER JOIN "PATENTS"."PATENTS"."PUBLICATIONS" p 
    ON cd.citing_pub = p."publication_number"
  LEFT JOIN LATERAL FLATTEN(INPUT => p."cpc", OUTER => TRUE) cpc
  WHERE cpc.index = 0
    AND p."filing_date" IS NOT NULL 
    AND p."filing_date" > 0
),
citing_with_assignees AS (
  SELECT 
    cwc.citing_pub,
    cwc.first_cpc_code,
    ass.value:"name"::STRING AS citing_assignee
  FROM citing_with_cpc cwc
  INNER JOIN "PATENTS"."PATENTS"."PUBLICATIONS" p 
    ON cwc.citing_pub = p."publication_number"
  LEFT JOIN LATERAL FLATTEN(INPUT => p."assignee_harmonized", OUTER => TRUE) ass
  WHERE ass.value:"name"::STRING IS NOT NULL
    AND ass.value:"name"::STRING NOT LIKE '%DENSO CORP%'
)
SELECT 
  cwa.citing_assignee,
  cd."titleFull" AS cpc_subclass_title,
  COUNT(*) AS citation_count
FROM citing_with_assignees cwa
LEFT JOIN "PATENTS"."PATENTS"."CPC_DEFINITION" cd 
  ON cwa.first_cpc_code = cd."symbol"
GROUP BY cwa.citing_assignee, cd."titleFull"
ORDER BY citation_count DESC