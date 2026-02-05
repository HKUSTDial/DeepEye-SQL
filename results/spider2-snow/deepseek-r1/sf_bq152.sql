WITH brca_cases AS (
    SELECT DISTINCT "case_barcode"
    FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."COPY_NUMBER_SEGMENT_MASKED"
    WHERE "project_short_name" = 'TCGA-BRCA'
),
cnv_segments AS (
    SELECT 
        "case_barcode",
        "chromosome",
        "start_pos",
        "end_pos",
        "segment_mean",
        2 * POWER(2, "segment_mean") AS "absolute_cn"
    FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."COPY_NUMBER_SEGMENT_MASKED"
    WHERE "project_short_name" = 'TCGA-BRCA'
),
chromosome_regions AS (
    SELECT 
        "chromosome",
        'p36.33' AS "cytoband_name",
        1 AS "region_start",
        2300000 AS "region_end"
    FROM (SELECT DISTINCT "chromosome" FROM cnv_segments)
    UNION ALL
    SELECT 
        "chromosome",
        'p36.32' AS "cytoband_name",
        2300001 AS "region_start",
        5300000 AS "region_end"
    FROM (SELECT DISTINCT "chromosome" FROM cnv_segments)
    UNION ALL
    SELECT 
        "chromosome",
        'p36.31' AS "cytoband_name",
        5300001 AS "region_start",
        7100000 AS "region_end"
    FROM (SELECT DISTINCT "chromosome" FROM cnv_segments)
),
overlap_calculations AS (
    SELECT 
        c."case_barcode",
        r."chromosome",
        r."cytoband_name",
        r."region_start",
        r."region_end",
        s."absolute_cn",
        GREATEST(0, LEAST(r."region_end", s."end_pos") - GREATEST(r."region_start", s."start_pos") + 1) AS "overlap_length"
    FROM brca_cases c
    CROSS JOIN chromosome_regions r
    LEFT JOIN cnv_segments s ON c."case_barcode" = s."case_barcode"
        AND r."chromosome" = s."chromosome"
        AND s."start_pos" <= r."region_end"
        AND s."end_pos" >= r."region_start"
),
weighted_averages AS (
    SELECT 
        "case_barcode",
        "chromosome",
        "cytoband_name",
        "region_start",
        "region_end",
        CASE 
            WHEN SUM("overlap_length") > 0 
            THEN SUM("absolute_cn" * "overlap_length") / SUM("overlap_length")
            ELSE 2.0
        END AS "weighted_avg_cn"
    FROM overlap_calculations
    GROUP BY 
        "case_barcode",
        "chromosome",
        "cytoband_name",
        "region_start",
        "region_end"
),
classified_data AS (
    SELECT 
        *,
        ROUND("weighted_avg_cn") AS "rounded_cn",
        CASE 
            WHEN ROUND("weighted_avg_cn") = 0 THEN 'homozygous_deletion'
            WHEN ROUND("weighted_avg_cn") = 1 THEN 'heterozygous_deletion'
            WHEN ROUND("weighted_avg_cn") = 2 THEN 'normal_diploid'
            WHEN ROUND("weighted_avg_cn") = 3 THEN 'gain'
            WHEN ROUND("weighted_avg_cn") > 3 THEN 'amplification'
            ELSE 'unknown'
        END AS "cnv_type"
    FROM weighted_averages
),
total_case_count AS (
    SELECT COUNT(*) AS "total_count" FROM brca_cases
)
SELECT 
    "chromosome",
    "cytoband_name",
    "region_start" AS "cytoband_start",
    "region_end" AS "cytoband_end",
    ROUND(100.0 * COUNT(CASE WHEN "cnv_type" = 'homozygous_deletion' THEN 1 END) / tc."total_count", 2) AS "homozygous_deletion_pct",
    ROUND(100.0 * COUNT(CASE WHEN "cnv_type" = 'heterozygous_deletion' THEN 1 END) / tc."total_count", 2) AS "heterozygous_deletion_pct",
    ROUND(100.0 * COUNT(CASE WHEN "cnv_type" = 'normal_diploid' THEN 1 END) / tc."total_count", 2) AS "normal_diploid_pct",
    ROUND(100.0 * COUNT(CASE WHEN "cnv_type" = 'gain' THEN 1 END) / tc."total_count", 2) AS "gain_pct",
    ROUND(100.0 * COUNT(CASE WHEN "cnv_type" = 'amplification' THEN 1 END) / tc."total_count", 2) AS "amplification_pct"
FROM classified_data
CROSS JOIN total_case_count tc
GROUP BY 
    "chromosome",
    "cytoband_name",
    "region_start",
    "region_end",
    tc."total_count"
ORDER BY 
    "chromosome",
    "region_start"