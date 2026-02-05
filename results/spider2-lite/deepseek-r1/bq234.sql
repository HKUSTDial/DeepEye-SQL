WITH state_drug_totals AS (
    SELECT `nppes_provider_state`, `generic_name`, SUM(`total_claim_count`) AS state_drug_total
    FROM `bigquery-public-data.cms_medicare.part_d_prescriber_2014`
    GROUP BY `nppes_provider_state`, `generic_name`
)
SELECT `nppes_provider_state`, `generic_name`, state_drug_total
FROM (
    SELECT `nppes_provider_state`, `generic_name`, state_drug_total,
           ROW_NUMBER() OVER (PARTITION BY `nppes_provider_state` ORDER BY state_drug_total DESC) AS rn
    FROM state_drug_totals
) WHERE rn = 1