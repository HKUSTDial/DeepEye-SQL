WITH filtered_cnv AS (
    SELECT "sample_barcode", "chromosome", "start_pos", "end_pos", "copy_number"
    FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23"
    WHERE "project_short_name" = 'TCGA-KIRC' AND "chromosome" = 'chr1'
), cytobands AS (
    SELECT "chromosome", "cytoband_name", "hg38_start", "hg38_stop"
    FROM "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38"
    WHERE "chromosome" = 'chr1'
), overlaps AS (
    SELECT c."sample_barcode", b."cytoband_name", c."copy_number"
    FROM filtered_cnv c
    JOIN cytobands b ON c."chromosome" = b."chromosome" AND c."start_pos" < b."hg38_stop" AND c."end_pos" > b."hg38_start"
), sample_cytoband_max AS (
    SELECT "sample_barcode", "cytoband_name", MAX("copy_number") AS "max_copy_number"
    FROM overlaps
    GROUP BY "sample_barcode", "cytoband_name"
), categorized AS (
    SELECT "sample_barcode", "cytoband_name", 
        CASE 
            WHEN "max_copy_number" > 3 THEN 'amplification'
            WHEN "max_copy_number" = 3 THEN 'gain'
            WHEN "max_copy_number" = 1 THEN 'het_del'
            ELSE 'other'
        END AS "category"
    FROM sample_cytoband_max
), counts_per_cytoband AS (
    SELECT "cytoband_name",
        COUNT(DISTINCT CASE WHEN "category" = 'amplification' THEN "sample_barcode" END) AS "amp_count",
        COUNT(DISTINCT CASE WHEN "category" = 'gain' THEN "sample_barcode" END) AS "gain_count",
        COUNT(DISTINCT CASE WHEN "category" = 'het_del' THEN "sample_barcode" END) AS "het_del_count"
    FROM categorized
    GROUP BY "cytoband_name"
), ranks AS (
    SELECT "cytoband_name", "amp_count", "gain_count", "het_del_count",
        DENSE_RANK() OVER (ORDER BY "amp_count" DESC) AS "amp_rank",
        DENSE_RANK() OVER (ORDER BY "gain_count" DESC) AS "gain_rank",
        DENSE_RANK() OVER (ORDER BY "het_del_count" DESC) AS "het_del_rank"
    FROM counts_per_cytoband
)
SELECT "cytoband_name"
FROM ranks
WHERE "amp_rank" <= 11 AND "gain_rank" <= 11 AND "het_del_rank" <= 11