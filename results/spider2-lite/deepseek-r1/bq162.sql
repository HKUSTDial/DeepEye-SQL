WITH filtered_level2 AS (
  SELECT 
    `HTAN_Data_File_ID`,
    `Imaging_Assay_Type`,
    `entityId` as level2_entityId
  FROM `isb-cgc-bq.HTAN_versioned.imaging_level2_metadata_r5`
  WHERE `HTAN_Center` = 'HTAN WUSTL'
    AND `Component` IS NOT NULL
    AND `Component` NOT LIKE '%Auxiliary%'
    AND `Component` NOT LIKE '%OtherAssay%'
    AND `Imaging_Assay_Type` != 'Electron Microscopy'
),
level2_provenance AS (
  SELECT DISTINCT
    f.`Imaging_Assay_Type`,
    f.`HTAN_Data_File_ID` as level2_file_id,
    ip.`entityId` as child_entity_id
  FROM filtered_level2 f
  JOIN `isb-cgc-bq.HTAN_versioned.id_provenance_r5` ip
    ON f.`HTAN_Data_File_ID` = ip.`HTAN_Parent_Data_File_ID`
  WHERE ip.`Component` IS NOT NULL
    AND ip.`Component` NOT LIKE '%Auxiliary%'
    AND ip.`Component` NOT LIKE '%OtherAssay%'
),
level3_data AS (
  SELECT DISTINCT
    lp.`Imaging_Assay_Type`
  FROM level2_provenance lp
  JOIN `isb-cgc-bq.HTAN_versioned.imaging_level3_segmentation_metadata_r5` l3
    ON lp.`child_entity_id` = l3.`entityId`
  WHERE l3.`Component` IS NOT NULL
    AND l3.`Component` NOT LIKE '%Auxiliary%'
    AND l3.`Component` NOT LIKE '%OtherAssay%'
),
level4_data AS (
  SELECT DISTINCT
    lp.`Imaging_Assay_Type`
  FROM level2_provenance lp
  JOIN `isb-cgc-bq.HTAN_versioned.imaging_level4_metadata_r5` l4
    ON lp.`child_entity_id` = l4.`entityId`
  WHERE l4.`Component` IS NOT NULL
    AND l4.`Component` NOT LIKE '%Auxiliary%'
    AND l4.`Component` NOT LIKE '%OtherAssay%'
)
SELECT DISTINCT
  f.`Imaging_Assay_Type`,
  CONCAT(
    'Level2',
    CASE WHEN l3.`Imaging_Assay_Type` IS NOT NULL THEN ', Level3' ELSE '' END,
    CASE WHEN l4.`Imaging_Assay_Type` IS NOT NULL THEN ', Level4' ELSE '' END
  ) as available_data_levels
FROM filtered_level2 f
LEFT JOIN level3_data l3 ON f.`Imaging_Assay_Type` = l3.`Imaging_Assay_Type`
LEFT JOIN level4_data l4 ON f.`Imaging_Assay_Type` = l4.`Imaging_Assay_Type`
ORDER BY f.`Imaging_Assay_Type`