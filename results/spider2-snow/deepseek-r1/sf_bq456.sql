SELECT 
    d."PatientID",
    d."StudyInstanceUID",
    d."StudyDate",
    q."findingSite":"CodeMeaning"::STRING AS finding_site_codemeaning,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Elongation' THEN q."Value" END) AS elongation_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Flatness' THEN q."Value" END) AS flatness_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Least Axis in 3D Length' THEN q."Value" END) AS least_axis_in_3d_length_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Major Axis in 3D Length' THEN q."Value" END) AS major_axis_in_3d_length_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Maximum 3D Diameter of a Mesh' THEN q."Value" END) AS maximum_3d_diameter_of_a_mesh_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Minor Axis in 3D Length' THEN q."Value" END) AS minor_axis_in_3d_length_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Sphericity' THEN q."Value" END) AS sphericity_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Surface Area of Mesh' THEN q."Value" END) AS surface_area_of_mesh_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Surface to Volume Ratio' THEN q."Value" END) AS surface_to_volume_ratio_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Volume from Voxel Summation' THEN q."Value" END) AS volume_from_voxel_summation_max,
    MAX(CASE WHEN q."Quantity":"CodeMeaning"::STRING = 'Volume of Mesh' THEN q."Value" END) AS volume_of_mesh_max
FROM "IDC"."IDC_V17"."DICOM_ALL" d
JOIN "IDC"."IDC_V17"."QUANTITATIVE_MEASUREMENTS" q ON q."segmentationInstanceUID" = d."SOPInstanceUID"
WHERE EXTRACT(YEAR FROM d."StudyDate") = 2001
    AND q."Quantity":"CodeMeaning"::STRING IN (
        'Elongation',
        'Flatness',
        'Least Axis in 3D Length',
        'Major Axis in 3D Length',
        'Maximum 3D Diameter of a Mesh',
        'Minor Axis in 3D Length',
        'Sphericity',
        'Surface Area of Mesh',
        'Surface to Volume Ratio',
        'Volume from Voxel Summation',
        'Volume of Mesh'
    )
GROUP BY d."PatientID", d."StudyInstanceUID", d."StudyDate", q."findingSite":"CodeMeaning"::STRING