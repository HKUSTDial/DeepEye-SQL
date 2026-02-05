WITH AnnotationsSet AS (
  SELECT DISTINCT
    case_gdc_id,
    case_barcode,
    category,
    classification
  FROM `isb-cgc.TCGA_bioclin_v0.Annotations`
  WHERE
    entity_type = 'Patient'
    AND (category = 'History of unacceptable prior treatment related to a prior/other malignancy' OR classification = 'Redaction')
),
ClinicalSet AS (
  SELECT DISTINCT
    case_gdc_id,
    case_barcode
  FROM `isb-cgc.TCGA_bioclin_v0.Clinical`
  WHERE
    disease_code = 'BRCA'
    AND age_at_diagnosis <= 30
    AND gender = 'FEMALE'
),
FilteredCases AS (
  SELECT
    COALESCE(A.case_gdc_id, C.case_gdc_id) AS case_gdc_id,
    COALESCE(A.case_barcode, C.case_barcode) AS case_barcode
  FROM AnnotationsSet AS A
  FULL JOIN ClinicalSet AS C
    ON A.case_gdc_id = C.case_gdc_id AND A.case_barcode = C.case_barcode
  WHERE
    A.category IS NULL AND A.classification IS NULL
)
SELECT DISTINCT
  CD.case_barcode,
  URL.file_gdc_url
FROM FilteredCases AS FC
INNER JOIN `isb-cgc.GDC_metadata.rel14_caseData` AS CD
  ON FC.case_gdc_id = CD.case_gdc_id
INNER JOIN `isb-cgc.GDC_metadata.rel14_fileData_current` AS FD
  ON CD.case_gdc_id = FD.case_gdc_id
INNER JOIN `isb-cgc.GDC_metadata.rel14_GDCfileID_to_GCSurl_NEW` AS URL
  ON FD.file_gdc_id = URL.file_gdc_id