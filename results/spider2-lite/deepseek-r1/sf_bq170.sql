WITH breast_cancer_cases AS (
    SELECT DISTINCT "case_barcode"
    FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23"
    WHERE "project_short_name" = 'TCGA-BRCA'
),
overlap_data AS (
    SELECT
        cn."case_barcode",
        cyto."cytoband_name",
        cyto."chromosome",
        cyto."hg38_start",
        cyto."hg38_stop",
        cn."start_pos",
        cn."end_pos",
        cn."copy_number",
        (ABS(cyto."hg38_stop" - cyto."hg38_start") + ABS(cn."end_pos" - cn."start_pos") - ABS(cyto."hg38_stop" - cn."end_pos") - ABS(cyto."hg38_start" - cn."start_pos")) / 2 AS "overlap"
    FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23" cn
    INNER JOIN "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38" cyto
        ON cn."chromosome" = cyto."chromosome"
    WHERE cn."project_short_name" = 'TCGA-BRCA'
        AND (ABS(cyto."hg38_stop" - cyto."hg38_start") + ABS(cn."end_pos" - cn."start_pos") - ABS(cyto."hg38_stop" - cn."end_pos") - ABS(cyto."hg38_start" - cn."start_pos")) / 2 > 0
),
weighted_avg_per_case AS (
    SELECT
        "case_barcode",
        "cytoband_name",
        "chromosome",
        "hg38_start",
        "hg38_stop",
        ROUND(SUM("overlap" * "copy_number") / SUM("overlap")) AS "rounded_copy_number"
    FROM overlap_data
    GROUP BY "case_barcode", "cytoband_name", "chromosome", "hg38_start", "hg38_stop"
),
classified AS (
    SELECT
        "case_barcode",
        "cytoband_name",
        "chromosome",
        "hg38_start",
        "hg38_stop",
        "rounded_copy_number",
        CASE
            WHEN "rounded_copy_number" = 0 THEN 'homozygous_deletion'
            WHEN "rounded_copy_number" = 1 THEN 'heterozygous_deletion'
            WHEN "rounded_copy_number" = 2 THEN 'normal_diploid'
            WHEN "rounded_copy_number" = 3 THEN 'gain'
            ELSE 'amplification'
        END AS "cnv_type"
    FROM weighted_avg_per_case
),
cytoband_summary AS (
    SELECT
        "cytoband_name",
        "chromosome",
        "hg38_start",
        "hg38_stop",
        "cnv_type",
        COUNT(DISTINCT "case_barcode") AS "case_count"
    FROM classified
    GROUP BY "cytoband_name", "chromosome", "hg38_start", "hg38_stop", "cnv_type"
),
total_cases AS (
    SELECT COUNT(*) AS "total_case_count" FROM breast_cancer_cases
)
SELECT
    cs."cytoband_name",
    cs."chromosome",
    cs."hg38_start",
    cs."hg38_stop",
    ROUND(COALESCE(SUM(CASE WHEN cs."cnv_type" = 'homozygous_deletion' THEN cs."case_count" ELSE 0 END) * 100.0 / tc."total_case_count", 0), 2) AS "homozygous_del_pct",
    ROUND(COALESCE(SUM(CASE WHEN cs."cnv_type" = 'heterozygous_deletion' THEN cs."case_count" ELSE 0 END) * 100.0 / tc."total_case_count", 0), 2) AS "heterozygous_del_pct",
    ROUND(COALESCE(SUM(CASE WHEN cs."cnv_type" = 'normal_diploid' THEN cs."case_count" ELSE 0 END) * 100.0 / tc."total_case_count", 0), 2) AS "normal_diploid_pct",
    ROUND(COALESCE(SUM(CASE WHEN cs."cnv_type" = 'gain' THEN cs."case_count" ELSE 0 END) * 100.0 / tc."total_case_count", 0), 2) AS "gain_pct",
    ROUND(COALESCE(SUM(CASE WHEN cs."cnv_type" = 'amplification' THEN cs."case_count" ELSE 0 END) * 100.0 / tc."total_case_count", 0), 2) AS "amplification_pct"
FROM cytoband_summary cs
CROSS JOIN total_cases tc
GROUP BY cs."cytoband_name", cs."chromosome", cs."hg38_start", cs."hg38_stop", tc."total_case_count"
ORDER BY cs."chromosome", cs."hg38_start"