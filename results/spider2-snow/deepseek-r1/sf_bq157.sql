WITH lgg_patients AS (
    SELECT DISTINCT "ParticipantBarcode"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Study" = 'LGG'
),
patient_drg2_avg AS (
    SELECT 
        exp."ParticipantBarcode",
        AVG(LOG(10, exp."normalized_count" + 1)) AS avg_log_expr
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."EBPP_ADJUSTPANCAN_ILLUMINAHISEQ_RNASEQV2_GENEXP_FILTERED" exp
    INNER JOIN "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE" maf
        ON exp."SampleBarcode" = maf."Tumor_SampleBarcode"
        AND exp."Study" = maf."Study"
    WHERE exp."Study" = 'LGG'
        AND exp."Symbol" = 'DRG2'
        AND exp."ParticipantBarcode" IN (SELECT "ParticipantBarcode" FROM lgg_patients)
    GROUP BY exp."ParticipantBarcode"
),
tp53_patients AS (
    SELECT DISTINCT "ParticipantBarcode"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Study" = 'LGG'
        AND "Hugo_Symbol" = 'TP53'
        AND "FILTER" = 'PASS'
),
group_stats AS (
    SELECT 
        CASE WHEN tp."ParticipantBarcode" IS NOT NULL THEN 'YES' ELSE 'NO' END AS has_tp53_mutation,
        COUNT(*) AS N,
        SUM(pda.avg_log_expr) AS S,
        SUM(POWER(pda.avg_log_expr, 2)) AS Q
    FROM patient_drg2_avg pda
    LEFT JOIN tp53_patients tp ON pda."ParticipantBarcode" = tp."ParticipantBarcode"
    GROUP BY CASE WHEN tp."ParticipantBarcode" IS NOT NULL THEN 'YES' ELSE 'NO' END
),
tp53_group AS (
    SELECT N AS Ny, S AS Sy, Q AS Qy
    FROM group_stats
    WHERE has_tp53_mutation = 'YES'
),
no_tp53_group AS (
    SELECT N AS Nn, S AS Sn, Q AS Qn
    FROM group_stats
    WHERE has_tp53_mutation = 'NO'
)
SELECT 
    tp53.Ny,
    no_tp53.Nn,
    tp53.Sy / tp53.Ny AS avg_y,
    no_tp53.Sn / no_tp53.Nn AS avg_n,
    (tp53.Sy / tp53.Ny - no_tp53.Sn / no_tp53.Nn) / 
    SQRT(
        ((tp53.Qy - POWER(tp53.Sy, 2) / tp53.Ny) / (tp53.Ny - 1)) / tp53.Ny +
        ((no_tp53.Qn - POWER(no_tp53.Sn, 2) / no_tp53.Nn) / (no_tp53.Nn - 1)) / no_tp53.Nn
    ) AS tscore
FROM tp53_group tp53
CROSS JOIN no_tp53_group no_tp53
WHERE tp53.Ny > 1 AND no_tp53.Nn > 1
    AND ((tp53.Qy - POWER(tp53.Sy, 2) / tp53.Ny) > 0)
    AND ((no_tp53.Qn - POWER(no_tp53.Sn, 2) / no_tp53.Nn) > 0)