SELECT DISTINCT
  COALESCE(
    NULLIF("journal"['title']::STRING, 'NULL'),
    NULLIF("proceedings_title"::STRING, 'NULL'),
    NULLIF("book_title"::STRING, 'NULL'),
    NULLIF("book_series_title"::STRING, 'NULL')
  ) AS venue_title
FROM "DIMENSIONS_AI_COVID19"."DIMENSIONS_AI_COVID19"."PUBLICATIONS" AS p
CROSS JOIN LATERAL FLATTEN(INPUT => p."research_orgs") AS orgs
INNER JOIN "DIMENSIONS_AI_COVID19"."DIMENSIONS_AI_COVID19"."GRID" AS g
  ON orgs.value::STRING = g."id"
WHERE EXTRACT(YEAR FROM TO_TIMESTAMP_NTZ(p."date_inserted" / 1000000)) >= 2021
  AND g."address"['city']::STRING = 'Qianjiang'