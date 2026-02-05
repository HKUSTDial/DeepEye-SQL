SELECT "case_barcode"
FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R35"
WHERE "project_short_name" = 'TCGA-BRCA' AND "gene_type" = 'protein_coding'
GROUP BY "case_barcode"
HAVING COUNT(DISTINCT "sample_type_name") >= 2 AND SUM(CASE WHEN "sample_type_name" = 'Solid Tissue Normal' THEN 1 ELSE 0 END) > 0