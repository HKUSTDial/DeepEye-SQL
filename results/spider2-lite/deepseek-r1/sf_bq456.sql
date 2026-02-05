SELECT
  d."PatientID",
  d."StudyInstanceUID",
  d."StudyDate",
  q."findingSite":"CodeMeaning"::varchar AS "FindingSite_CodeMeaning",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Elongation' THEN q."Value" END) AS "Max_Elongation",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Flatness' THEN q."Value" END) AS "Max_Flatness",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Least Axis in 3D Length' THEN q."Value" END) AS "Max_Least Axis in 3D Length",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Major Axis in 3D Length' THEN q."Value" END) AS "Max_Major Axis in 3D Length",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Maximum 3D Diameter of a Mesh' THEN q."Value" END) AS "Max_Maximum 3D Diameter of a Mesh",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Minor Axis in 3D Length' THEN q."Value" END) AS "Max_Minor Axis in 3D Length",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Sphericity' THEN q."Value" END) AS "Max_Sphericity",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Surface Area of Mesh' THEN q."Value" END) AS "Max_Surface Area of Mesh",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Surface to Volume Ratio' THEN q."Value" END) AS "Max_Surface to Volume Ratio",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Volume from Voxel Summation' THEN q."Value" END) AS "Max_Volume from Voxel Summation",
  MAX(CASE WHEN q."Quantity":"CodeMeaning"::varchar = 'Volume of Mesh' THEN q."Value" END) AS "Max_Volume of Mesh"
FROM "IDC"."IDC_V17"."DICOM_ALL" d
JOIN "IDC"."IDC_V17"."QUANTITATIVE_MEASUREMENTS" q
  ON q."segmentationInstanceUID" = d."SOPInstanceUID"
WHERE EXTRACT(YEAR FROM d."StudyDate") = 2001
  AND q."Quantity":"CodeMeaning"::varchar IN ('Elongation', 'Flatness', 'Least Axis in 3D Length', 'Major Axis in 3D Length', 'Maximum 3D Diameter of a Mesh', 'Minor Axis in 3D Length', 'Sphericity', 'Surface Area of Mesh', 'Surface to Volume Ratio', 'Volume from Voxel Summation', 'Volume of Mesh')
GROUP BY d."PatientID", d."StudyInstanceUID", d."StudyDate", q."findingSite":"CodeMeaning"::varchar