WITH per_variant_sample AS (
    SELECT
        v."start",
        v."end",
        v."reference_name",
        call_obj.value:"call_set_name"::TEXT AS "sample_id",
        call_obj.value:"genotype"[0]::INT + call_obj.value:"genotype"[1]::INT AS "alt_allele_count",
        2 - (call_obj.value:"genotype"[0]::INT + call_obj.value:"genotype"[1]::INT) AS "ref_allele_count"
    FROM
        "_1000_GENOMES"."_1000_GENOMES"."VARIANTS" v,
        LATERAL FLATTEN(input => v."call") call_obj
    WHERE
        v."reference_name" = '12'
        AND v."call" IS NOT NULL
        AND ARRAY_SIZE(v."alternate_bases") = 1
        AND call_obj.value:"genotype"[0]::INT IN (0,1)
        AND call_obj.value:"genotype"[1]::INT IN (0,1)
        AND call_obj.value:"genotype"[0] IS NOT NULL
        AND call_obj.value:"genotype"[1] IS NOT NULL
),
per_variant_pop AS (
    SELECT
        pvs."start",
        pvs."end",
        pvs."sample_id",
        pvs."alt_allele_count",
        pvs."ref_allele_count",
        si."Super_Population",
        CASE WHEN si."Super_Population" = 'EAS' THEN 'case' ELSE 'control' END AS group_type
    FROM
        per_variant_sample pvs
        JOIN "_1000_GENOMES"."_1000_GENOMES"."SAMPLE_INFO" si
            ON pvs."sample_id" = si."Sample"
),
variant_group_counts AS (
    SELECT
        "start",
        "end",
        group_type,
        SUM("alt_allele_count") AS a,
        SUM("ref_allele_count") AS r
    FROM
        per_variant_pop
    GROUP BY
        "start", "end", group_type
),
variant_contingency AS (
    SELECT
        "start",
        "end",
        COALESCE(MAX(CASE WHEN group_type = 'case' THEN a END), 0) AS a,
        COALESCE(MAX(CASE WHEN group_type = 'control' THEN a END), 0) AS b,
        COALESCE(MAX(CASE WHEN group_type = 'case' THEN r END), 0) AS c,
        COALESCE(MAX(CASE WHEN group_type = 'control' THEN r END), 0) AS d
    FROM
        variant_group_counts
    GROUP BY
        "start", "end"
),
variant_expected AS (
    SELECT
        "start",
        "end",
        a,
        b,
        c,
        d,
        a + b + c + d AS N,
        (a+b)*(a+c)/NULLIF(N,0) AS e_a,
        (a+b)*(b+d)/NULLIF(N,0) AS e_b,
        (c+d)*(a+c)/NULLIF(N,0) AS e_c,
        (c+d)*(b+d)/NULLIF(N,0) AS e_d
    FROM
        variant_contingency
    WHERE
        N > 0
        AND (a+b) > 0 AND (c+d) > 0 AND (a+c) > 0 AND (b+d) > 0
),
variant_chi AS (
    SELECT
        "start",
        "end",
        a,
        b,
        c,
        d,
        N,
        e_a,
        e_b,
        e_c,
        e_d,
        POWER(ABS(a*d - b*c) - N/2.0, 2) * N / ((a+b)*(c+d)*(a+c)*(b+d)) AS chi_squared
    FROM
        variant_expected
    WHERE
        e_a >= 5 AND e_b >= 5 AND e_c >= 5 AND e_d >= 5
)
SELECT
    "start",
    "end",
    chi_squared
FROM
    variant_chi
WHERE
    chi_squared >= 29.71679
ORDER BY
    chi_squared DESC