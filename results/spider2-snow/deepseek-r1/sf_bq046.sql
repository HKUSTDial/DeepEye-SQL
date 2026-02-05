WITH "annotations_set" AS (
    SELECT "case_gdc_id", "case_barcode", "category", "classification"
    FROM "TCGA_BIOCLIN_V0"."TCGA_BIOCLIN_V0"."ANNOTATIONS"
    WHERE "entity_type" = 'Patient'
    AND ("category" = 'History of unacceptable prior treatment related to a prior/other malignancy' OR "classification" = 'Redaction')
), "clinical_set" AS (
    SELECT "case_gdc_id", "case_barcode"
    FROM "TCGA_BIOCLIN_V0"."TCGA_BIOCLIN_V0"."CLINICAL"
    WHERE "disease_code" = 'BRCA'
    AND "age_at_diagnosis" <= 30
    AND "gender" = 'FEMALE'
), "filtered_cases" AS (
    SELECT COALESCE("a"."case_gdc_id", "c"."case_gdc_id") AS "case_gdc_id",
           COALESCE("a"."case_barcode", "c"."case_barcode") AS "case_barcode",
           "a"."category",
           "a"."classification"
    FROM "clinical_set" AS "c"
    FULL JOIN "annotations_set" AS "a" ON "c"."case_gdc_id" = "a"."case_gdc_id"
    WHERE "a"."category" IS NULL AND "a"."classification" IS NULL
)
SELECT DISTINCT "cd"."case_barcode", "url"."file_gdc_url"
FROM "filtered_cases" AS "fc"
INNER JOIN "TCGA_BIOCLIN_V0"."GDC_METADATA"."REL14_CASEDATA" AS "cd" ON "fc"."case_gdc_id" = "cd"."case_gdc_id"
INNER JOIN "TCGA_BIOCLIN_V0"."GDC_METADATA"."REL14_FILEDATA_CURRENT" AS "fd" ON "fc"."case_gdc_id" = "fd"."case_gdc_id"
INNER JOIN "TCGA_BIOCLIN_V0"."GDC_METADATA"."REL14_GDCFILEID_TO_GCSURL_NEW" AS "url" ON "fd"."file_gdc_id" = "url"."file_gdc_id"