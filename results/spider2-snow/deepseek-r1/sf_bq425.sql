WITH sanofi_products AS (
  SELECT
    "product_id",
    "trade_name",
    "approval_date"
  FROM "EBI_CHEMBL"."EBI_CHEMBL"."PRODUCTS_23"
  WHERE "applicant_full_name" = 'SanofiAventis'
),
product_molecules AS (
  SELECT
    sp."product_id",
    sp."trade_name",
    sp."approval_date",
    f."molregno"
  FROM sanofi_products sp
  JOIN "EBI_CHEMBL"."EBI_CHEMBL"."FORMULATIONS_23" f
    ON sp."product_id" = f."product_id"
),
molecule_info AS (
  SELECT
    pm."product_id",
    pm."trade_name",
    pm."approval_date",
    pm."molregno",
    md."chembl_id"
  FROM product_molecules pm
  JOIN "EBI_CHEMBL"."EBI_CHEMBL"."MOLECULE_DICTIONARY_23" md
    ON pm."molregno" = md."molregno"
),
ranked_molecules AS (
  SELECT
    "chembl_id",
    "molregno",
    "trade_name",
    "approval_date",
    ROW_NUMBER() OVER (PARTITION BY "molregno" ORDER BY TO_DATE("approval_date") DESC) AS rn
  FROM molecule_info
)
SELECT
  "chembl_id",
  "trade_name",
  "approval_date"
FROM ranked_molecules
WHERE rn = 1
ORDER BY "chembl_id"