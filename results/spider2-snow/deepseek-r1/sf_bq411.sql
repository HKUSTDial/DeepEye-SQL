SELECT "refresh_date", "term", "rank"
FROM "GOOGLE_TRENDS"."GOOGLE_TRENDS"."TOP_TERMS"
WHERE "refresh_date" BETWEEN '2024-09-01' AND '2024-09-14'
  AND DAYOFWEEKISO("refresh_date") BETWEEN 1 AND 5
  AND "rank" IN (1, 2, 3)
ORDER BY "refresh_date" DESC, "rank" ASC