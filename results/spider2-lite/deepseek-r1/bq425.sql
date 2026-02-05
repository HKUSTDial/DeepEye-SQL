WITH sanofi_molecules AS (
  SELECT DISTINCT ms.molregno
  FROM `bigquery-public-data.ebi_chembl.research_companies_23` rc
  JOIN `bigquery-public-data.ebi_chembl.molecule_synonyms_23` ms ON rc.res_stem_id = ms.res_stem_id
  WHERE rc.company = 'SanofiAventis'
),
molecule_info AS (
  SELECT md.chembl_id, md.pref_name, md.molregno
  FROM `bigquery-public-data.ebi_chembl.molecule_dictionary_23` md
  JOIN sanofi_molecules sm ON md.molregno = sm.molregno
),
product_rank AS (
  SELECT f.molregno, p.trade_name, p.approval_date,
    ROW_NUMBER() OVER (PARTITION BY f.molregno ORDER BY PARSE_DATE('%Y-%m-%d', p.approval_date) DESC, p.trade_name) AS rn
  FROM `bigquery-public-data.ebi_chembl.formulations_23` f
  JOIN `bigquery-public-data.ebi_chembl.products_23` p ON f.product_id = p.product_id
  WHERE p.approval_date IS NOT NULL AND p.approval_date != ''
    AND f.molregno IN (SELECT molregno FROM sanofi_molecules)
),
latest_product AS (
  SELECT molregno, trade_name, approval_date
  FROM product_rank
  WHERE rn = 1
)
SELECT mi.chembl_id, mi.pref_name, lp.trade_name, lp.approval_date
FROM molecule_info mi
JOIN latest_product lp ON mi.molregno = lp.molregno
ORDER BY mi.chembl_id