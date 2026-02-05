WITH activity_counts AS (
  SELECT 
    molregno,
    assay_id,
    standard_type,
    COUNT(*) as act_count,
    SUM(CASE WHEN potential_duplicate = '1' THEN 1 ELSE 0 END) as dup_count
  FROM `bigquery-public-data.ebi_chembl.activities`
  WHERE standard_value IS NOT NULL 
    AND standard_value != 'NULL'
    AND pchembl_value IS NOT NULL
    AND SAFE_CAST(pchembl_value AS FLOAT64) > 10
  GROUP BY molregno, assay_id, standard_type
  HAVING act_count < 5 AND dup_count < 2
),
filtered_activities AS (
  SELECT 
    a.activity_id,
    a.molregno,
    a.assay_id,
    a.standard_type,
    a.standard_value,
    a.standard_relation,
    a.doc_id,
    SAFE_CAST(a.pchembl_value AS FLOAT64) as pchembl_val,
    SAFE_CAST(cp.heavy_atoms AS INT64) as heavy_atoms
  FROM `bigquery-public-data.ebi_chembl.activities` a
  JOIN `bigquery-public-data.ebi_chembl.compound_properties` cp 
    ON a.molregno = cp.molregno
  JOIN activity_counts ac 
    ON a.molregno = ac.molregno 
    AND a.assay_id = ac.assay_id 
    AND a.standard_type = ac.standard_type
  WHERE SAFE_CAST(cp.heavy_atoms AS INT64) BETWEEN 10 AND 15
    AND a.standard_value IS NOT NULL 
    AND a.standard_value != 'NULL'
    AND a.pchembl_value IS NOT NULL
    AND SAFE_CAST(a.pchembl_value AS FLOAT64) > 10
),
doc_dates AS (
  SELECT 
    doc_id,
    journal,
    CASE 
      WHEN year IS NULL OR year = 'NULL' THEN 1970
      ELSE SAFE_CAST(year AS INT64)
    END as pub_year,
    CASE 
      WHEN first_page IS NULL OR first_page = 'NULL' THEN 0
      ELSE SAFE_CAST(REGEXP_EXTRACT(first_page, r'^(\d+)') AS INT64)
    END as first_page_num
  FROM `bigquery-public-data.ebi_chembl.docs`
),
doc_ranks AS (
  SELECT 
    doc_id,
    journal,
    pub_year,
    PERCENT_RANK() OVER (PARTITION BY journal, pub_year ORDER BY first_page_num) as pct_rank
  FROM doc_dates
  WHERE pub_year IS NOT NULL
),
pub_dates AS (
  SELECT 
    doc_id,
    DATE(
      pub_year,
      CAST(FLOOR(COALESCE(pct_rank, 0) * 11) + 1 AS INT64),
      CAST(MOD(CAST(FLOOR(COALESCE(pct_rank, 0) * 308) AS INT64), 28) + 1 AS INT64)
    ) as pub_date
  FROM doc_ranks
),
pairs AS (
  SELECT 
    a1.molregno as molregno1,
    a2.molregno as molregno2,
    a1.assay_id,
    a1.standard_type,
    a1.activity_id as activity_id1,
    a2.activity_id as activity_id2,
    SAFE_CAST(a1.standard_value AS FLOAT64) as std_val1,
    SAFE_CAST(a2.standard_value AS FLOAT64) as std_val2,
    a1.standard_relation as rel1,
    a2.standard_relation as rel2,
    a1.doc_id as doc_id1,
    a2.doc_id as doc_id2,
    a1.heavy_atoms as heavy_atoms1,
    a2.heavy_atoms as heavy_atoms2,
    cs1.canonical_smiles as smiles1,
    cs2.canonical_smiles as smiles2
  FROM filtered_activities a1
  JOIN filtered_activities a2 
    ON a1.assay_id = a2.assay_id 
    AND a1.standard_type = a2.standard_type
    AND a1.molregno < a2.molregno
  JOIN `bigquery-public-data.ebi_chembl.compound_structures` cs1 
    ON a1.molregno = cs1.molregno
  JOIN `bigquery-public-data.ebi_chembl.compound_structures` cs2 
    ON a2.molregno = cs2.molregno
)
SELECT 
  TO_HEX(MD5(TO_JSON_STRING(STRUCT(activity_id1, activity_id2)))) as activity_pair_uuid,
  TO_HEX(MD5(TO_JSON_STRING(STRUCT(smiles1, smiles2)))) as smiles_pair_uuid,
  GREATEST(heavy_atoms1, heavy_atoms2) as max_heavy_atoms,
  GREATEST(pd1.pub_date, pd2.pub_date) as latest_publication_date,
  GREATEST(SAFE_CAST(doc_id1 AS INT64), SAFE_CAST(doc_id2 AS INT64)) as highest_document_id,
  CASE 
    WHEN rel1 = '=' AND rel2 = '=' THEN
      CASE 
        WHEN std_val1 > std_val2 THEN 'decrease'
        WHEN std_val1 < std_val2 THEN 'increase'
        ELSE 'no-change'
      END
    WHEN (rel1 IN ('>', '>=') OR rel2 IN ('<', '<=')) AND std_val1 > std_val2 THEN 'decrease'
    WHEN (rel1 IN ('<', '<=') OR rel2 IN ('>', '>=')) AND std_val1 < std_val2 THEN 'increase'
    ELSE 'no-change'
  END as standard_change_classification
FROM pairs
JOIN pub_dates pd1 ON pairs.doc_id1 = pd1.doc_id
JOIN pub_dates pd2 ON pairs.doc_id2 = pd2.doc_id
WHERE pd1.pub_date IS NOT NULL AND pd2.pub_date IS NOT NULL