WITH gene_expr AS (
    SELECT 
        "ParticipantBarcode",
        AVG(LOG(10, "normalized_count" + 1)) as transformed_expr
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."EBPP_ADJUSTPANCAN_ILLUMINAHISEQ_RNASEQV2_GENEXP_FILTERED"
    WHERE "Study" = 'LGG'
      AND "Symbol" = 'IGF2'
      AND "normalized_count" IS NOT NULL
    GROUP BY "ParticipantBarcode"
),
clinical_data AS (
    SELECT 
        "bcr_patient_barcode" as "ParticipantBarcode",
        "icd_o_3_histology"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP_FILTERED"
    WHERE "acronym" = 'LGG'
      AND "icd_o_3_histology" IS NOT NULL
      AND NOT REGEXP_LIKE("icd_o_3_histology", '^\\[.*\\]$')
),
joined_data AS (
    SELECT 
        g."ParticipantBarcode",
        g.transformed_expr,
        c."icd_o_3_histology"
    FROM gene_expr g
    JOIN clinical_data c ON g."ParticipantBarcode" = c."ParticipantBarcode"
),
ranked_data AS (
    SELECT 
        "icd_o_3_histology",
        transformed_expr,
        AVG(rank_val) OVER (PARTITION BY transformed_expr) as avg_rank
    FROM (
        SELECT 
            "icd_o_3_histology",
            transformed_expr,
            ROW_NUMBER() OVER (ORDER BY transformed_expr) as rank_val
        FROM joined_data
    ) t
),
group_stats AS (
    SELECT 
        "icd_o_3_histology",
        COUNT(*) as n_i,
        SUM(avg_rank) as S_i,
        SUM(avg_rank * avg_rank) as Q_i
    FROM ranked_data
    GROUP BY "icd_o_3_histology"
    HAVING COUNT(*) > 1
),
overall_stats AS (
    SELECT 
        SUM(n_i) as N,
        SUM(S_i) as sum_S_i,
        SUM(Q_i) as sum_Q_i,
        SUM(S_i * S_i / n_i) as sum_S_i2_over_n_i
    FROM group_stats
)
SELECT 
    (SELECT COUNT(*) FROM group_stats) as total_number_of_groups,
    o.N as total_number_of_samples,
    (o.N - 1) * (o.sum_S_i2_over_n_i - (o.sum_S_i * o.sum_S_i) / o.N) / (o.sum_Q_i - (o.sum_S_i * o.sum_S_i) / o.N) as kruskal_wallis_h_score
FROM overall_stats o
ORDER BY kruskal_wallis_h_score DESC