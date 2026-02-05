WITH autosomal_common_variants AS (
    SELECT 
        "reference_name",
        "start",
        "end", 
        "reference_bases",
        "alternate_bases",
        "AF",
        "call"
    FROM "_1000_GENOMES"."_1000_GENOMES"."VARIANTS"
    WHERE "reference_name" NOT IN ('X', 'Y', 'MT')
    AND "AF" >= 0.05
),
flattened_calls AS (
    SELECT
        v."reference_name",
        v."start",
        v."end",
        v."reference_bases",
        v."alternate_bases",
        f.value:"sample_id"::STRING AS sample_id,
        f.value:"genotype"::ARRAY AS genotype_array
    FROM autosomal_common_variants v,
    LATERAL FLATTEN(INPUT => v."call") f
    WHERE ARRAY_SIZE(f.value:"genotype"::ARRAY) > 0
    AND (f.value:"genotype"[0]::INT > 0 OR f.value:"genotype"[1]::INT > 0)
),
samples_with_variants AS (
    SELECT DISTINCT
        fc."reference_name",
        fc."start", 
        fc."end",
        fc."reference_bases",
        fc."alternate_bases",
        fc.sample_id,
        si."Super_Population"
    FROM flattened_calls fc
    JOIN "_1000_GENOMES"."_1000_GENOMES"."SAMPLE_INFO" si
    ON fc.sample_id = si."Sample"
),
variant_sample_counts AS (
    SELECT
        "Super_Population",
        "reference_name",
        "start",
        "end",
        "reference_bases",
        "alternate_bases",
        COUNT(DISTINCT sample_id) AS samples_with_variant_count
    FROM samples_with_variants
    GROUP BY 
        "Super_Population",
        "reference_name",
        "start",
        "end",
        "reference_bases",
        "alternate_bases"
),
superpop_totals AS (
    SELECT
        "Super_Population",
        COUNT(DISTINCT "Sample") AS total_population_size
    FROM "_1000_GENOMES"."_1000_GENOMES"."SAMPLE_INFO"
    GROUP BY "Super_Population"
),
variant_counts_by_sample_count AS (
    SELECT
        vsc."Super_Population",
        vsc.samples_with_variant_count,
        COUNT(DISTINCT 
            vsc."reference_name" || '|' || 
            vsc."start" || '|' || 
            vsc."end" || '|' || 
            vsc."reference_bases"
        ) AS variant_count
    FROM variant_sample_counts vsc
    GROUP BY vsc."Super_Population", vsc.samples_with_variant_count
)
SELECT
    vc."Super_Population",
    sp.total_population_size,
    TRUE AS is_common,
    vc.samples_with_variant_count AS number_of_samples_having_variant,
    vc.variant_count AS total_variants_shared_by_that_many_samples
FROM variant_counts_by_sample_count vc
JOIN superpop_totals sp ON vc."Super_Population" = sp."Super_Population"
ORDER BY vc."Super_Population", vc.samples_with_variant_count