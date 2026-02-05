SELECT COUNT(*)
FROM (
    SELECT p."publication_number"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" p
    LEFT JOIN LATERAL FLATTEN(input => p."claims_localized") claims
    WHERE p."country_code" = 'US'
      AND p."kind_code" = 'B2'
      AND p."grant_date" BETWEEN 20080101 AND 20181231
    GROUP BY p."publication_number"
    HAVING COUNT(claims.value) > 0
       AND SUM(CASE WHEN claims.value:"text"::TEXT ILIKE '%claim%' THEN 1 ELSE 0 END) = 0
) AS patents_without_claim