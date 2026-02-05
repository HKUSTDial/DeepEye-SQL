WITH proteomics AS (
    SELECT
        p."gene_symbol",
        p."protein_abundance_log2ratio",
        m."sample_submitter_id"
    FROM "CPTAC_PDC"."CPTAC"."QUANT_PROTEOME_CPTAC_CCRCC_DISCOVERY_STUDY_PDC_CURRENT" p
    INNER JOIN "CPTAC_PDC"."PDC_METADATA"."ALIQUOT_TO_CASE_MAPPING_CURRENT" m
        ON p."aliquot_submitter_id" = m."aliquot_submitter_id"
),
rnaseq AS (
    SELECT
        "gene_name",
        "sample_barcode",
        "sample_type_name",
        AVG("fpkm_unstranded") AS avg_fpkm
    FROM "CPTAC_PDC"."CPTAC"."RNASEQ_HG38_GDC_CURRENT"
    WHERE "primary_site" = 'Kidney'
        AND "sample_type_name" IN ('Primary Tumor', 'Solid Tissue Normal')
        AND "project_short_name" LIKE 'CPTAC%'
    GROUP BY "gene_name", "sample_barcode", "sample_type_name"
),
joined_data AS (
    SELECT
        prot."gene_symbol",
        rna."sample_type_name",
        prot."protein_abundance_log2ratio",
        LOG(2, rna.avg_fpkm + 1) AS log2_fpkm
    FROM proteomics prot
    INNER JOIN rnaseq rna
        ON prot."sample_submitter_id" = rna."sample_barcode"
        AND prot."gene_symbol" = rna."gene_name"
),
correlations AS (
    SELECT
        "gene_symbol",
        "sample_type_name",
        CORR("protein_abundance_log2ratio", log2_fpkm) AS correlation
    FROM joined_data
    GROUP BY "gene_symbol", "sample_type_name"
),
filtered_correlations AS (
    SELECT
        "gene_symbol",
        "sample_type_name",
        correlation
    FROM correlations
    WHERE ABS(correlation) <= 0.5
)
SELECT
    "sample_type_name",
    AVG(correlation) AS avg_correlation
FROM filtered_correlations
GROUP BY "sample_type_name"
ORDER BY "sample_type_name"