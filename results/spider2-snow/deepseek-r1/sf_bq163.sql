SELECT
  "FEATURE_NAME",
  AVG(CASE WHEN "SEX" = 'female' THEN "X_VALUE" END) AS avg_female,
  AVG(CASE WHEN "SEX" = 'male' THEN "X_VALUE" END) AS avg_male,
  avg_female - avg_male AS difference
FROM "HTAN_2"."HTAN"."SCRNASEQ_MSK_SCLC_COMBINED_SAMPLES_CURRENT"
WHERE "CLUSTERS" = '41'
  AND "DEVELOPMENT_STAGE" = '74-year-old human stage'
  AND "CELL_TYPE_GENERAL" = 'Epithelial'
  AND "SEX" IN ('female', 'male')
GROUP BY "FEATURE_NAME"
HAVING avg_female IS NOT NULL AND avg_male IS NOT NULL
ORDER BY ABS(difference) DESC
LIMIT 20