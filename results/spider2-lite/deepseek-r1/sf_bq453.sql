WITH filtered_variants AS (
    SELECT
        "reference_name",
        "start",
        "end",
        "reference_bases",
        "alternate_bases",
        "VT",
        TRY_TO_DOUBLE("AF") AS "AF",
        TRY_TO_DOUBLE("EUR_AF") AS "EUR_AF",
        TRY_TO_DOUBLE("ASN_AF") AS "ASN_AF",
        TRY_TO_DOUBLE("AFR_AF") AS "AFR_AF",
        TRY_TO_DOUBLE("AMR_AF") AS "AMR_AF",
        "call"
    FROM "_1000_GENOMES"."_1000_GENOMES"."VARIANTS"
    WHERE "reference_name" = '17'
        AND "start" >= 41196311
        AND "start" <= 41277499
),
genotype_counts AS (
    SELECT
        fv."reference_name",
        fv."start",
        fv."end",
        fv."reference_bases",
        fv."alternate_bases",
        fv."VT",
        fv."AF",
        fv."EUR_AF",
        fv."ASN_AF",
        fv."AFR_AF",
        fv."AMR_AF",
        COUNT(cf.value) AS total_genotypes,
        COUNT_IF(
            cf.value:GT[0]::INTEGER = 0 AND cf.value:GT[1]::INTEGER = 0
        ) AS obs_hom_ref,
        COUNT_IF(
            (cf.value:GT[0]::INTEGER = 0 AND cf.value:GT[1]::INTEGER > 0) 
            OR (cf.value:GT[0]::INTEGER > 0 AND cf.value:GT[1]::INTEGER = 0)
            OR (cf.value:GT[0]::INTEGER > 0 AND cf.value:GT[1]::INTEGER > 0 AND cf.value:GT[0]::INTEGER != cf.value:GT[1]::INTEGER)
        ) AS obs_het,
        COUNT_IF(
            cf.value:GT[0]::INTEGER > 0 AND cf.value:GT[1]::INTEGER > 0 AND cf.value:GT[0]::INTEGER = cf.value:GT[1]::INTEGER
        ) AS obs_hom_alt
    FROM filtered_variants fv
    LEFT JOIN LATERAL FLATTEN(INPUT => fv."call") cf
    GROUP BY
        fv."reference_name",
        fv."start",
        fv."end",
        fv."reference_bases",
        fv."alternate_bases",
        fv."VT",
        fv."AF",
        fv."EUR_AF",
        fv."ASN_AF",
        fv."AFR_AF",
        fv."AMR_AF"
),
distinct_alternates AS (
    SELECT
        "reference_name",
        "start",
        "end",
        "reference_bases",
        "alternate_bases",
        LISTAGG(DISTINCT ab.value::STRING, ',') WITHIN GROUP (ORDER BY ab.value::STRING) AS "distinct_alternate_bases"
    FROM filtered_variants,
    LATERAL FLATTEN(INPUT => "alternate_bases") ab
    GROUP BY
        "reference_name",
        "start",
        "end",
        "reference_bases",
        "alternate_bases"
)
SELECT
    gc."reference_name",
    gc."start",
    gc."end",
    gc."reference_bases",
    da."distinct_alternate_bases",
    gc."VT",
    gc."AF" AS "allele_frequency",
    gc."EUR_AF",
    gc."ASN_AF",
    gc."AFR_AF",
    gc."AMR_AF",
    gc.total_genotypes,
    gc.obs_hom_ref,
    gc.obs_het,
    gc.obs_hom_alt,
    CASE
        WHEN gc."AF" IS NOT NULL THEN gc."AF"
        WHEN gc.total_genotypes > 0 THEN (2.0 * gc.obs_hom_alt + gc.obs_het) / (2.0 * gc.total_genotypes)
        ELSE NULL
    END AS p_alt,
    CASE
        WHEN gc."AF" IS NOT NULL THEN gc."AF"
        WHEN gc.total_genotypes > 0 THEN (2.0 * gc.obs_hom_alt + gc.obs_het) / (2.0 * gc.total_genotypes)
        ELSE NULL
    END * CASE
        WHEN gc."AF" IS NOT NULL THEN gc."AF"
        WHEN gc.total_genotypes > 0 THEN (2.0 * gc.obs_hom_alt + gc.obs_het) / (2.0 * gc.total_genotypes)
        ELSE NULL
    END * gc.total_genotypes AS exp_hom_alt,
    2.0 * CASE
        WHEN gc."AF" IS NOT NULL THEN gc."AF"
        WHEN gc.total_genotypes > 0 THEN (2.0 * gc.obs_hom_alt + gc.obs_het) / (2.0 * gc.total_genotypes)
        ELSE NULL
    END * (1 - CASE
        WHEN gc."AF" IS NOT NULL THEN gc."AF"
        WHEN gc.total_genotypes > 0 THEN (2.0 * gc.obs_hom_alt + gc.obs_het) / (2.0 * gc.total_genotypes)
        ELSE NULL
    END) * gc.total_genotypes AS exp_het,
    (1 - CASE
        WHEN gc."AF" IS NOT NULL THEN gc."AF"
        WHEN gc.total_genotypes > 0 THEN (2.0 * gc.obs_hom_alt + gc.obs_het) / (2.0 * gc.total_genotypes)
        ELSE NULL
    END) * (1 - CASE
        WHEN gc."AF" IS NOT NULL THEN gc."AF"
        WHEN gc.total_genotypes > 0 THEN (2.0 * gc.obs_hom_alt + gc.obs_het) / (2.0 * gc.total_genotypes)
        ELSE NULL
    END) * gc.total_genotypes AS exp_hom_ref,
    CASE
        WHEN exp_hom_alt > 0 THEN POWER(gc.obs_hom_alt - exp_hom_alt, 2) / exp_hom_alt
        ELSE 0
    END + CASE
        WHEN exp_het > 0 THEN POWER(gc.obs_het - exp_het, 2) / exp_het
        ELSE 0
    END + CASE
        WHEN exp_hom_ref > 0 THEN POWER(gc.obs_hom_ref - exp_hom_ref, 2) / exp_hom_ref
        ELSE 0
    END AS chi_squared_score
FROM genotype_counts gc
INNER JOIN distinct_alternates da
    ON gc."reference_name" = da."reference_name"
    AND gc."start" = da."start"
    AND gc."end" = da."end"
    AND gc."reference_bases" = da."reference_bases"
    AND gc."alternate_bases" = da."alternate_bases"
ORDER BY gc."start"