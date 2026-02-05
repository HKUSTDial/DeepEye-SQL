WITH sorafenib_drugs AS (
  SELECT DISTINCT drugID
  FROM `isb-cgc-bq.targetome_versioned.drug_synonyms_v1`
  WHERE LOWER(synonym) LIKE '%sorafenib%'
),
targets AS (
  SELECT DISTINCT i.target_uniprotID
  FROM `isb-cgc-bq.targetome_versioned.interactions_v1` i
  JOIN `isb-cgc-bq.targetome_versioned.experiments_v1` e ON i.expID = e.expID
  WHERE i.targetSpecies = 'Homo sapiens'
    AND i.drugID IN (SELECT drugID FROM sorafenib_drugs)
    AND e.exp_assayValueMedian <= 100
    AND (e.exp_assayValueLow <= 100 OR e.exp_assayValueLow IS NULL)
    AND (e.exp_assayValueHigh <= 100 OR e.exp_assayValueHigh IS NULL)
),
pathway_proteins AS (
  SELECT p.stable_id AS pathway_id, p.name AS pathway_name, pe.uniprot_id
  FROM `isb-cgc-bq.reactome_versioned.pathway_v77` p
  JOIN `isb-cgc-bq.reactome_versioned.pe_to_pathway_v77` pp ON p.stable_id = pp.pathway_stable_id
  JOIN `isb-cgc-bq.reactome_versioned.physical_entity_v77` pe ON pp.pe_stable_id = pe.stable_id
  WHERE pp.evidence_code = 'TAS'
    AND p.species = 'Homo sapiens'
    AND p.lowest_level = TRUE
),
total_counts AS (
  SELECT 
    COUNT(DISTINCT target_uniprotID) AS total_targets,
    (SELECT COUNT(DISTINCT uniprot_id) FROM pathway_proteins) AS total_background
  FROM targets
),
pathway_counts AS (
  SELECT 
    pp.pathway_id,
    pp.pathway_name,
    COUNT(DISTINCT CASE WHEN t.target_uniprotID IS NOT NULL THEN pp.uniprot_id END) AS targets_inside,
    COUNT(DISTINCT pp.uniprot_id) AS background_inside
  FROM pathway_proteins pp
  LEFT JOIN targets t ON pp.uniprot_id = t.target_uniprotID
  GROUP BY pp.pathway_id, pp.pathway_name
),
pathway_stats AS (
  SELECT 
    pc.*,
    tc.total_targets,
    tc.total_background,
    (tc.total_targets - pc.targets_inside) AS targets_outside,
    (pc.background_inside - pc.targets_inside) AS non_targets_inside,
    (tc.total_background - pc.background_inside - tc.total_targets + pc.targets_inside) AS non_targets_outside
  FROM pathway_counts pc
  CROSS JOIN total_counts tc
),
chi_squared AS (
  SELECT 
    *,
    (POW((targets_inside * non_targets_outside - targets_outside * non_targets_inside), 2) * 
     (targets_inside + targets_outside + non_targets_inside + non_targets_outside)) /
     NULLIF((targets_inside + targets_outside) * (non_targets_inside + non_targets_outside) * 
            (targets_inside + non_targets_inside) * (targets_outside + non_targets_outside), 0) AS chi_sq
  FROM pathway_stats
),
top_pathways AS (
  SELECT *
  FROM chi_squared
  ORDER BY chi_sq DESC
  LIMIT 3
)
SELECT 
  pathway_id,
  pathway_name,
  targets_inside,
  targets_outside,
  non_targets_inside,
  non_targets_outside
FROM top_pathways