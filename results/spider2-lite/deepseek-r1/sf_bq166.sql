WITH total_cases AS (
    SELECT COUNT(DISTINCT "case_barcode") as total_cases
    FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23"
    WHERE "project_short_name" = 'TCGA-KIRC'
),
overlapping_segments AS (
    SELECT 
        cns."case_barcode",
        cns."sample_barcode",
        cns."chromosome",
        cns."copy_number",
        cb."cytoband_name"
    FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23" cns
    JOIN "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38" cb
        ON cns."chromosome" = cb."chromosome"
        AND cns."start_pos" <= cb."hg38_stop"
        AND cns."end_pos" >= cb."hg38_start"
    WHERE cns."project_short_name" = 'TCGA-KIRC'
),
max_per_sample_cytoband AS (
    SELECT 
        "case_barcode",
        "sample_barcode",
        "chromosome",
        "cytoband_name",
        MAX("copy_number") as max_copy_number
    FROM overlapping_segments
    GROUP BY "case_barcode", "sample_barcode", "chromosome", "cytoband_name"
),
classified AS (
    SELECT 
        "case_barcode",
        "chromosome",
        "cytoband_name",
        max_copy_number,
        CASE
            WHEN max_copy_number > 3 THEN 'amplification'
            WHEN max_copy_number = 3 THEN 'gain'
            WHEN max_copy_number = 0 THEN 'homozygous deletion'
            WHEN max_copy_number = 1 THEN 'heterozygous deletion'
            WHEN max_copy_number = 2 THEN 'normal'
            ELSE 'other'
        END as category
    FROM max_per_sample_cytoband
),
category_counts AS (
    SELECT 
        "chromosome",
        "cytoband_name",
        category,
        COUNT(DISTINCT "case_barcode") as case_count
    FROM classified
    GROUP BY "chromosome", "cytoband_name", category
)
SELECT 
    cc."chromosome",
    cc."cytoband_name",
    cc.category,
    ROUND(cc.case_count * 100.0 / tc.total_cases, 2) as frequency_percentage
FROM category_counts cc
CROSS JOIN total_cases tc
ORDER BY cc."chromosome", cc."cytoband_name"