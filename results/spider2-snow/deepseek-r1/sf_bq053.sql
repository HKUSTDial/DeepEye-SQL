SELECT 
  t."fall_color",
  SUM(COALESCE(c1995.count_1995, 0) - COALESCE(c2015.count_2015, 0)) as total_change
FROM "NEW_YORK"."NEW_YORK"."TREE_SPECIES" t
LEFT JOIN (
  SELECT UPPER("spc_latin") as sci_name_upper, COUNT(*) as count_1995
  FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_1995"
  WHERE "status" != 'Dead' AND "spc_latin" != 'PLANTING SITE'
  GROUP BY UPPER("spc_latin")
) c1995 ON UPPER(t."species_scientific_name") = c1995.sci_name_upper
LEFT JOIN (
  SELECT UPPER("spc_latin") as sci_name_upper, COUNT(*) as count_2015
  FROM "NEW_YORK"."NEW_YORK"."TREE_CENSUS_2015"
  WHERE "status" = 'Alive'
  GROUP BY UPPER("spc_latin")
) c2015 ON UPPER(t."species_scientific_name") = c2015.sci_name_upper
GROUP BY t."fall_color"