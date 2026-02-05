WITH sorafenib_drug AS (
  SELECT DISTINCT "drugID"
  FROM "TARGETOME_REACTOME"."TARGETOME_VERSIONED"."DRUG_SYNONYMS_V1"
  WHERE LOWER("synonym") LIKE '%sorafenib%'
  UNION
  SELECT DISTINCT "drugID"
  FROM "TARGETOME_REACTOME"."TARGETOME_VERSIONED"."INTERACTIONS_V1"
  WHERE LOWER("drugName") LIKE '%sorafenib%'
),
interest_targets AS (
  SELECT DISTINCT i."target_uniprotID"
  FROM "TARGETOME_REACTOME"."TARGETOME_VERSIONED"."INTERACTIONS_V1" i
  INNER JOIN "TARGETOME_REACTOME"."TARGETOME_VERSIONED"."EXPERIMENTS_V1" e ON i."expID" = e."expID"
  WHERE i."drugID" IN (SELECT "drugID" FROM sorafenib_drug)
    AND i."targetSpecies" = 'Homo sapiens'
    AND e."exp_assayValueMedian" <= 100
    AND (e."exp_assayValueLow" <= 100 OR e."exp_assayValueLow" IS NULL)
    AND (e."exp_assayValueHigh" <= 100 OR e."exp_assayValueHigh" IS NULL)
),
background_targets AS (
  SELECT DISTINCT "target_uniprotID"
  FROM "TARGETOME_REACTOME"."TARGETOME_VERSIONED"."INTERACTIONS_V1"
  WHERE "targetSpecies" = 'Homo sapiens'
),
pathways AS (
  SELECT "stable_id", "name"
  FROM "TARGETOME_REACTOME"."REACTOME_VERSIONED"."PATHWAY_V77"
  WHERE "lowest_level" = TRUE
    AND "species" = 'Homo sapiens'
),
pathway_targets AS (
  SELECT DISTINCT ppt."pathway_stable_id", pe."uniprot_id"
  FROM "TARGETOME_REACTOME"."REACTOME_VERSIONED"."PE_TO_PATHWAY_V77" ppt
  INNER JOIN "TARGETOME_REACTOME"."REACTOME_VERSIONED"."PHYSICAL_ENTITY_V77" pe ON ppt."pe_stable_id" = pe."stable_id"
  INNER JOIN pathways p ON ppt."pathway_stable_id" = p."stable_id"
  WHERE ppt."evidence_code" = 'TAS'
    AND pe."uniprot_id" IS NOT NULL
),
all_targets AS (
  SELECT bt."target_uniprotID", CASE WHEN it."target_uniprotID" IS NOT NULL THEN 1 ELSE 0 END AS is_interest
  FROM background_targets bt
  LEFT JOIN interest_targets it ON bt."target_uniprotID" = it."target_uniprotID"
),
pathway_counts AS (
  SELECT pt."pathway_stable_id", COUNT(CASE WHEN at.is_interest = 1 THEN 1 END) AS interest_in_pathway, COUNT(CASE WHEN at.is_interest = 0 THEN 1 END) AS noninterest_in_pathway
  FROM pathway_targets pt
  INNER JOIN all_targets at ON pt."uniprot_id" = at."target_uniprotID"
  GROUP BY pt."pathway_stable_id"
),
interest_total AS (SELECT COUNT(*) AS K FROM interest_targets),
background_total AS (SELECT COUNT(*) AS N FROM background_targets)
SELECT p."stable_id" AS pathway_stable_id, p."name" AS pathway_name, pc.interest_in_pathway AS targets_in_pathway, it.K - pc.interest_in_pathway AS targets_outside_pathway, pc.noninterest_in_pathway AS non_targets_in_pathway, (bt.N - it.K) - pc.noninterest_in_pathway AS non_targets_outside_pathway, bt.N * POWER(pc.interest_in_pathway * ((bt.N - it.K) - pc.noninterest_in_pathway) - (it.K - pc.interest_in_pathway) * pc.noninterest_in_pathway, 2) / NULLIF(it.K * (bt.N - it.K) * (pc.interest_in_pathway + pc.noninterest_in_pathway) * (bt.N - (pc.interest_in_pathway + pc.noninterest_in_pathway)), 0) AS chi_squared
FROM pathway_counts pc
INNER JOIN pathways p ON pc."pathway_stable_id" = p."stable_id"
CROSS JOIN interest_total it
CROSS JOIN background_total bt
ORDER BY chi_squared DESC NULLS LAST
LIMIT 3