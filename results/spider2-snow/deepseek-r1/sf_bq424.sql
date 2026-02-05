SELECT d."country_name", SUM(d."value") AS "total_debt"
FROM "WORLD_BANK"."WORLD_BANK_INTL_DEBT"."INTERNATIONAL_DEBT" d
JOIN "WORLD_BANK"."WORLD_BANK_INTL_DEBT"."SERIES_SUMMARY" s ON d."indicator_code" = s."series_code"
JOIN "WORLD_BANK"."WORLD_BANK_INTL_DEBT"."COUNTRY_SUMMARY" c ON d."country_code" = c."country_code"
WHERE s."topic" ILIKE '%external debt%' AND s."indicator_name" ILIKE '%long-term%' AND c."region" IS NOT NULL
GROUP BY d."country_code", d."country_name"
ORDER BY "total_debt" DESC
LIMIT 10