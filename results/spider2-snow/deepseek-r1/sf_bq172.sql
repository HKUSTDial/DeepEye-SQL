SELECT "nppes_provider_state", SUM("total_claim_count") AS total_claim_count, SUM("total_drug_cost") AS total_drug_cost
FROM "CMS_DATA"."CMS_MEDICARE"."PART_D_PRESCRIBER_2014"
WHERE "drug_name" = (
    SELECT "drug_name"
    FROM "CMS_DATA"."CMS_MEDICARE"."PART_D_PRESCRIBER_2014"
    WHERE "nppes_provider_state" = 'NY'
    GROUP BY "drug_name"
    ORDER BY SUM("total_claim_count") DESC
    LIMIT 1
)
GROUP BY "nppes_provider_state"
ORDER BY total_claim_count DESC
LIMIT 5