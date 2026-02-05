WITH level2_files AS (
    SELECT "HTAN_Data_File_ID", "Imaging_Assay_Type"
    FROM "HTAN_1"."HTAN_VERSIONED"."IMAGING_LEVEL2_METADATA_R5"
    WHERE "HTAN_Center" = 'HTAN WUSTL'
        AND "Component" IS NOT NULL
        AND "Component" NOT LIKE '%Auxiliary%'
        AND "Component" NOT LIKE '%OtherAssay%'
        AND "Imaging_Assay_Type" IS NOT NULL
        AND "Imaging_Assay_Type" NOT LIKE '%Electron Microscopy%'
),
level2_assay_types AS (
    SELECT DISTINCT "Imaging_Assay_Type" FROM level2_files
),
level3_assay_types AS (
    SELECT DISTINCT l2."Imaging_Assay_Type"
    FROM level2_files l2
    JOIN "HTAN_1"."HTAN_VERSIONED"."ID_PROVENANCE_R5" p
        ON l2."HTAN_Data_File_ID" = p."HTAN_PARENT_DATA_FILE_ID"
    JOIN "HTAN_1"."HTAN_VERSIONED"."IMAGING_LEVEL3_SEGMENTATION_METADATA_R5" l3
        ON p."HTAN_DATA_FILE_ID" = l3."HTAN_Data_File_ID"
    WHERE p."COMPONENT" IS NOT NULL
        AND p."COMPONENT" NOT LIKE '%Auxiliary%'
        AND p."COMPONENT" NOT LIKE '%OtherAssay%'
        AND l3."Component" IS NOT NULL
        AND l3."Component" NOT LIKE '%Auxiliary%'
        AND l3."Component" NOT LIKE '%OtherAssay%'
),
level4_assay_types AS (
    SELECT DISTINCT l2."Imaging_Assay_Type"
    FROM level2_files l2
    JOIN "HTAN_1"."HTAN_VERSIONED"."ID_PROVENANCE_R5" p
        ON l2."HTAN_Data_File_ID" = p."HTAN_PARENT_DATA_FILE_ID"
    JOIN "HTAN_1"."HTAN_VERSIONED"."IMAGING_LEVEL4_METADATA_R5" l4
        ON p."HTAN_DATA_FILE_ID" = l4."HTAN_Data_File_ID"
    WHERE p."COMPONENT" IS NOT NULL
        AND p."COMPONENT" NOT LIKE '%Auxiliary%'
        AND p."COMPONENT" NOT LIKE '%OtherAssay%'
        AND l4."Component" IS NOT NULL
        AND l4."Component" NOT LIKE '%Auxiliary%'
        AND l4."Component" NOT LIKE '%OtherAssay%'
)
SELECT
    l2."Imaging_Assay_Type",
    'Level2' ||
    CASE WHEN l3."Imaging_Assay_Type" IS NOT NULL THEN ',Level3' ELSE '' END ||
    CASE WHEN l4."Imaging_Assay_Type" IS NOT NULL THEN ',Level4' ELSE '' END AS available_data_levels
FROM level2_assay_types l2
LEFT JOIN level3_assay_types l3 ON l2."Imaging_Assay_Type" = l3."Imaging_Assay_Type"
LEFT JOIN level4_assay_types l4 ON l2."Imaging_Assay_Type" = l4."Imaging_Assay_Type"
ORDER BY l2."Imaging_Assay_Type"