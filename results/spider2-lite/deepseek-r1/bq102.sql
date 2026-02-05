SELECT DISTINCT `start_position`
FROM `bigquery-public-data.gnomAD.v2_1_1_exomes__chr17`,
UNNEST(`alternate_bases`) AS `alt`,
UNNEST(`alt`.`vep`) AS `vep`
WHERE `reference_name` = '17'
  AND `reference_bases` = 'C'
  AND `alt`.`alt` = 'T'
  AND `vep`.`SYMBOL` = 'BRCA1'
  AND CONTAINS_SUBSTR(`vep`.`Consequence`, 'missense_variant')
UNION DISTINCT
SELECT DISTINCT `start_position`
FROM `bigquery-public-data.gnomAD.v2_1_1_genomes__chr17`,
UNNEST(`alternate_bases`) AS `alt`,
UNNEST(`alt`.`vep`) AS `vep`
WHERE `reference_name` = '17'
  AND `reference_bases` = 'C'
  AND `alt`.`alt` = 'T'
  AND `vep`.`SYMBOL` = 'BRCA1'
  AND CONTAINS_SUBSTR(`vep`.`Consequence`, 'missense_variant')