WITH gene_region AS (
  SELECT 
    MIN("start_position") AS min_start,
    MAX("end_position") AS max_end
  FROM "GNOMAD"."GNOMAD"."V2_1_1_GENOMES__CHR17",
  LATERAL FLATTEN(INPUT => "alternate_bases":vep) vep
  WHERE vep.value:"symbol" = 'BRCA1'
),
missense_variants AS (
  SELECT 
    vep_anno.value:"Protein_position" AS Protein_position
  FROM "GNOMAD"."GNOMAD"."V2_1_1_GENOMES__CHR17" var,
  LATERAL FLATTEN(INPUT => var."alternate_bases":vep) vep_anno,
  gene_region
  WHERE var."start_position" >= gene_region.min_start
    AND var."end_position" <= gene_region.max_end
    AND vep_anno.value:"Consequence" LIKE '%missense_variant%'
)
SELECT Protein_position
FROM missense_variants
WHERE Protein_position IS NOT NULL
ORDER BY CAST(Protein_position AS INTEGER)
LIMIT 1