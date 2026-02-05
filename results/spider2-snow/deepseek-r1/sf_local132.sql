WITH entertainer_styles_ranked AS (
  SELECT 
    "EntertainerID",
    "StyleID",
    ROW_NUMBER() OVER (PARTITION BY "EntertainerID" ORDER BY "StyleStrength" ASC) AS strength_rank,
    COUNT(*) OVER (PARTITION BY "EntertainerID") AS style_count
  FROM "ENTERTAINMENTAGENCY"."ENTERTAINMENTAGENCY"."ENTERTAINER_STYLES"
),
entertainer_first_second AS (
  SELECT 
    "EntertainerID",
    MAX(CASE WHEN strength_rank = 1 THEN "StyleID" END) AS first_style,
    MAX(CASE WHEN strength_rank = 2 THEN "StyleID" END) AS second_style
  FROM entertainer_styles_ranked
  WHERE style_count <= 3 AND strength_rank <= 2
  GROUP BY "EntertainerID"
  HAVING COUNT(*) >= 2
),
customer_preferences_ranked AS (
  SELECT 
    "CustomerID",
    "StyleID",
    ROW_NUMBER() OVER (PARTITION BY "CustomerID" ORDER BY "PreferenceSeq" ASC) AS pref_rank,
    COUNT(*) OVER (PARTITION BY "CustomerID") AS pref_count
  FROM "ENTERTAINMENTAGENCY"."ENTERTAINMENTAGENCY"."MUSICAL_PREFERENCES"
),
customer_first_second AS (
  SELECT 
    "CustomerID",
    MAX(CASE WHEN pref_rank = 1 THEN "StyleID" END) AS first_pref,
    MAX(CASE WHEN pref_rank = 2 THEN "StyleID" END) AS second_pref
  FROM customer_preferences_ranked
  WHERE pref_count <= 3 AND pref_rank <= 2
  GROUP BY "CustomerID"
  HAVING COUNT(*) >= 2
)
SELECT 
  e."EntStageName",
  c."CustLastName"
FROM entertainer_first_second efs
JOIN customer_first_second cfs
  ON (efs.first_style = cfs.first_pref AND efs.second_style = cfs.second_pref)
  OR (efs.first_style = cfs.second_pref AND efs.second_style = cfs.first_pref)
JOIN "ENTERTAINMENTAGENCY"."ENTERTAINMENTAGENCY"."ENTERTAINERS" e
  ON efs."EntertainerID" = e."EntertainerID"
JOIN "ENTERTAINMENTAGENCY"."ENTERTAINMENTAGENCY"."CUSTOMERS" c
  ON cfs."CustomerID" = c."CustomerID"