WITH 
-- Get participants in LGG with mutations and their samples
lgg_mc3 AS (
    SELECT DISTINCT "ParticipantBarcode", "Tumor_SampleBarcode"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Study" = 'LGG'
),
-- Get DRG2 expression for LGG samples that are in MC3
drg2_expr AS (
    SELECT 
        e."ParticipantBarcode",
        e."SampleBarcode",
        LOG(10, e."normalized_count" + 1) as log_expr
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."EBPP_ADJUSTPANCAN_ILLUMINAHISEQ_RNASEQV2_GENEXP_FILTERED" e
    INNER JOIN lgg_mc3 m ON e."SampleBarcode" = m."Tumor_SampleBarcode"
    WHERE e."Study" = 'LGG' AND e."Symbol" = 'DRG2'
),
-- Average log expression per participant
avg_expr_per_patient AS (
    SELECT 
        "ParticipantBarcode",
        AVG(log_expr) as avg_log_expr
    FROM drg2_expr
    GROUP BY "ParticipantBarcode"
),
-- Participants with TP53 mutation (PASS)
tp53_patients AS (
    SELECT DISTINCT "ParticipantBarcode"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Study" = 'LGG' 
        AND "Hugo_Symbol" = 'TP53' 
        AND "FILTER" = 'PASS'
),
-- Combine: label each patient as YES/NO for TP53 mutation
combined_data AS (
    SELECT 
        a."ParticipantBarcode",
        a.avg_log_expr,
        CASE WHEN t."ParticipantBarcode" IS NOT NULL THEN 'YES' ELSE 'NO' END as has_tp53
    FROM avg_expr_per_patient a
    LEFT JOIN tp53_patients t ON a."ParticipantBarcode" = t."ParticipantBarcode"
),
-- Compute sums and counts for YES group
yes_group AS (
    SELECT 
        COUNT(*) as Ny,
        SUM(avg_log_expr) as Sy,
        SUM(avg_log_expr * avg_log_expr) as Qy
    FROM combined_data
    WHERE has_tp53 = 'YES'
),
-- Compute sums and counts for NO group
no_group AS (
    SELECT 
        COUNT(*) as Nn,
        SUM(avg_log_expr) as Sn,
        SUM(avg_log_expr * avg_log_expr) as Qn
    FROM combined_data
    WHERE has_tp53 = 'NO'
),
-- Compute means and variances
stats AS (
    SELECT 
        y.Ny,
        n.Nn,
        y.Sy / y.Ny as avg_y,
        n.Sn / n.Nn as avg_n,
        (y.Qy - (y.Sy * y.Sy) / y.Ny) / (y.Ny - 1) as s_y2,
        (n.Qn - (n.Sn * n.Sn) / n.Nn) / (n.Nn - 1) as s_n2
    FROM yes_group y, no_group n
    WHERE y.Ny > 1 AND n.Nn > 1
)
-- Compute T-score
SELECT 
    Ny,
    Nn,
    avg_y,
    avg_n,
    (avg_y - avg_n) / SQRT(s_y2 / Ny + s_n2 / Nn) as tscore
FROM stats
WHERE s_y2 > 0 AND s_n2 > 0