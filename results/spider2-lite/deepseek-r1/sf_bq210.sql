SELECT COUNT(*) AS patent_count
FROM (
    SELECT p."publication_number"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" AS p
    LEFT JOIN LATERAL FLATTEN(INPUT => p."claims_localized_html") AS "CLAIMS"
    WHERE p."country_code" = 'US'
      AND p."kind_code" = 'B2'
      AND p."grant_date" BETWEEN 20080101 AND 20181231
    GROUP BY p."publication_number"
    HAVING COUNT("CLAIMS"."VALUE") > 0
       AND SUM(CASE WHEN "CLAIMS"."VALUE"::STRING ILIKE '%claim%' THEN 1 ELSE 0 END) = 0
) AS sub;