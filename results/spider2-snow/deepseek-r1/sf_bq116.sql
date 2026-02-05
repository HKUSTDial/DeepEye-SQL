SELECT "submission"."stprba" AS "state", SUM("numbers"."VALUE") / 1000000000 AS "total_revenue_billions"
FROM "SEC_QUARTERLY_FINANCIALS"."SEC_QUARTERLY_FINANCIALS"."SUBMISSION" AS "submission"
JOIN "SEC_QUARTERLY_FINANCIALS"."SEC_QUARTERLY_FINANCIALS"."NUMBERS" AS "numbers"
ON "submission"."submission_number" = "numbers"."SUBMISSION_NUMBER"
WHERE "submission"."fiscal_year" = 2016
AND "submission"."countryba" = 'US'
AND "submission"."stprba" IS NOT NULL
AND "submission"."stprba" != ''
AND "numbers"."MEASURE_TAG" IN ('Revenues', 'SalesRevenueNet', 'SalesRevenueGoodsNet')
AND "numbers"."NUMBER_OF_QUARTERS" = 4
AND "numbers"."UNITS" = 'USD'
AND "numbers"."IPRX" = 1
AND "numbers"."COREGISTRANT" IS NULL
GROUP BY "submission"."stprba"
ORDER BY "total_revenue_billions" DESC
LIMIT 1