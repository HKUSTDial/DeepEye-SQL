WITH gene_region AS (
  SELECT MIN(start_position) AS min_start, MAX(end_position) AS max_end
  FROM `bigquery-public-data.gnomAD.v2_1_1_genomes__chr17`,
  UNNEST(alternate_bases) AS alt,
  UNNEST(alt.vep) AS vep
  WHERE vep.SYMBOL = 'BRCA1'
)
SELECT DISTINCT vep.Protein_position
FROM `bigquery-public-data.gnomAD.v2_1_1_genomes__chr17` AS var,
  UNNEST(alternate_bases) AS alt,
  UNNEST(alt.vep) AS vep,
  gene_region
WHERE var.start_position >= gene_region.min_start
  AND var.end_position <= gene_region.max_end
  AND vep.Consequence LIKE '%missense_variant%'
  AND SAFE_CAST(vep.Protein_position AS INT64) IS NOT NULL
ORDER BY SAFE_CAST(vep.Protein_position AS INT64)
LIMIT 1