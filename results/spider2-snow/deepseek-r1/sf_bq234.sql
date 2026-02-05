SELECT "state", "drug_name", "total_claims"
FROM (
    SELECT "nppes_provider_state" AS "state", "drug_name",
           SUM("total_claim_count") AS "total_claims",
           RANK() OVER (PARTITION BY "nppes_provider_state" ORDER BY SUM("total_claim_count") DESC) AS "rnk"
    FROM "CMS_DATA"."CMS_MEDICARE"."PART_D_PRESCRIBER_2014"
    GROUP BY "nppes_provider_state", "drug_name"
) AS "ranked"
WHERE "rnk" = 1
ORDER BY "state", "drug_name"