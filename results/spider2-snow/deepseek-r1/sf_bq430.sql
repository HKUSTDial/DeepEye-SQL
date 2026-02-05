WITH docs_with_defaults AS (
  SELECT "doc_id", "journal", COALESCE(NULLIF("year", ''), '1970') AS "year", "first_page"
  FROM "EBI_CHEMBL"."EBI_CHEMBL"."DOCS"
),
doc_ranks AS (
  SELECT "doc_id", "journal", "year", "first_page",
         CAST("first_page" AS INTEGER) AS first_page_int,
         PERCENT_RANK() OVER (PARTITION BY "journal", "year" ORDER BY CAST("first_page" AS INTEGER)) AS pct_rank
  FROM docs_with_defaults
  WHERE "first_page" IS NOT NULL AND "first_page" != '' AND TRY_CAST("first_page" AS INTEGER) IS NOT NULL
),
doc_dates AS (
  SELECT "doc_id",
         DATE_FROM_PARTS(
           CAST("year" AS INTEGER),
           COALESCE(FLOOR(pct_rank * 11) + 1, 1),
           COALESCE((FLOOR(pct_rank * 308) % 28) + 1, 1)
         ) AS publication_date
  FROM doc_ranks
),
filtered_activities AS (
  SELECT a."activity_id", a."molregno", a."assay_id", a."standard_type",
         a."standard_value", a."standard_relation", a."pchembl_value",
         a."doc_id",
         cp."heavy_atoms",
         cs."canonical_smiles"
  FROM "EBI_CHEMBL"."EBI_CHEMBL"."ACTIVITIES" a
  INNER JOIN "EBI_CHEMBL"."EBI_CHEMBL"."COMPOUND_PROPERTIES" cp
    ON a."molregno" = cp."molregno"
  INNER JOIN "EBI_CHEMBL"."EBI_CHEMBL"."COMPOUND_STRUCTURES" cs
    ON a."molregno" = cs."molregno"
  WHERE a."standard_value" IS NOT NULL AND a."standard_value" != 'NULL'
    AND TRY_CAST(a."standard_value" AS NUMERIC) IS NOT NULL
    AND a."pchembl_value" IS NOT NULL AND a."pchembl_value" != 'NULL'
    AND TRY_CAST(a."pchembl_value" AS NUMERIC) > 10
    AND a."standard_relation" = '='
    AND cp."heavy_atoms" IS NOT NULL AND cp."heavy_atoms" != 'NULL'
    AND TRY_CAST(cp."heavy_atoms" AS INTEGER) BETWEEN 10 AND 15
),
activity_counts AS (
  SELECT "molregno", "assay_id",
         COUNT(*) AS total_activities,
         COUNT(CASE WHEN "potential_duplicate" != '0' THEN 1 END) AS duplicate_activities
  FROM "EBI_CHEMBL"."EBI_CHEMBL"."ACTIVITIES"
  GROUP BY "molregno", "assay_id"
),
valid_activities AS (
  SELECT fa."activity_id", fa."molregno", fa."assay_id", fa."standard_type",
         fa."standard_value", fa."standard_relation", fa."pchembl_value",
         fa."doc_id", fa."heavy_atoms", fa."canonical_smiles",
         ac.total_activities, ac.duplicate_activities,
         dd.publication_date
  FROM filtered_activities fa
  INNER JOIN activity_counts ac
    ON fa."molregno" = ac."molregno" AND fa."assay_id" = ac."assay_id"
  LEFT JOIN doc_dates dd
    ON fa."doc_id" = dd."doc_id"
  WHERE ac.total_activities < 5 AND ac.duplicate_activities < 2
),
paired AS (
  SELECT 
    A."activity_id" AS activity_id_A,
    B."activity_id" AS activity_id_B,
    A."molregno" AS molregno_A,
    B."molregno" AS molregno_B,
    A."assay_id",
    A."standard_type",
    A."heavy_atoms" AS heavy_atoms_A,
    B."heavy_atoms" AS heavy_atoms_B,
    A."standard_value" AS standard_value_A,
    B."standard_value" AS standard_value_B,
    A."standard_relation" AS standard_relation_A,
    B."standard_relation" AS standard_relation_B,
    A.publication_date AS publication_date_A,
    B.publication_date AS publication_date_B,
    A."doc_id" AS doc_id_A,
    B."doc_id" AS doc_id_B,
    A."canonical_smiles" AS canonical_smiles_A,
    B."canonical_smiles" AS canonical_smiles_B
  FROM valid_activities A
  INNER JOIN valid_activities B
    ON A."assay_id" = B."assay_id"
    AND A."standard_type" = B."standard_type"
    AND A."molregno" < B."molregno"
)
SELECT
  GREATEST(CAST("heavy_atoms_A" AS INTEGER), CAST("heavy_atoms_B" AS INTEGER)) AS max_heavy_atom_count,
  GREATEST("publication_date_A", "publication_date_B") AS latest_publication_date,
  GREATEST(CAST("doc_id_A" AS INTEGER), CAST("doc_id_B" AS INTEGER)) AS highest_document_id,
  CASE 
    WHEN CAST("standard_value_A" AS NUMERIC) > CAST("standard_value_B" AS NUMERIC) THEN 'decrease'
    WHEN CAST("standard_value_A" AS NUMERIC) < CAST("standard_value_B" AS NUMERIC) THEN 'increase'
    ELSE 'no-change'
  END AS change_classification,
  TO_HEX(MD5(TO_JSONSTRING(ARRAY_CONSTRUCT("activity_id_A", "activity_id_B")))) AS uuid_activity_ids,
  TO_HEX(MD5(TO_JSONSTRING(ARRAY_CONSTRUCT("canonical_smiles_A", "canonical_smiles_B")))) AS uuid_canonical_smiles
FROM paired