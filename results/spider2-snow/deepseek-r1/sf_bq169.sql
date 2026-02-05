WITH qualifying_clones AS (
  SELECT "RefNo", "CaseNo", "InvNo", "Clone"
  FROM "MITELMAN"."PROD"."CYTOCONVERTED"
  WHERE ("Type" = 'Loss' AND "ChrOrd" = 13 AND "Start" <= 48481890 AND "End" >= 48303751)
     OR ("Type" = 'Loss' AND "ChrOrd" = 17 AND "Start" <= 7687490 AND "End" >= 7668421)
     OR ("Type" = 'Gain' AND "ChrOrd" = 11 AND "Start" <= 108369102 AND "End" >= 108223067)
  GROUP BY "RefNo", "CaseNo", "InvNo", "Clone"
  HAVING COUNT(DISTINCT CASE WHEN "Type" = 'Loss' AND "ChrOrd" = 13 AND "Start" <= 48481890 AND "End" >= 48303751 THEN 1 WHEN "Type" = 'Loss' AND "ChrOrd" = 17 AND "Start" <= 7687490 AND "End" >= 7668421 THEN 2 WHEN "Type" = 'Gain' AND "ChrOrd" = 11 AND "Start" <= 108369102 AND "End" >= 108223067 THEN 3 END) = 3
), cc13_details AS (
  SELECT "RefNo", "CaseNo", "InvNo", "Clone", "ChrOrd", "Start", "End", ROW_NUMBER() OVER (PARTITION BY "RefNo", "CaseNo", "InvNo", "Clone" ORDER BY "Start") AS rn
  FROM "MITELMAN"."PROD"."CYTOCONVERTED"
  WHERE "Type" = 'Loss' AND "ChrOrd" = 13 AND "Start" <= 48481890 AND "End" >= 48303751
), cc17_details AS (
  SELECT "RefNo", "CaseNo", "InvNo", "Clone", "ChrOrd", "Start", "End", ROW_NUMBER() OVER (PARTITION BY "RefNo", "CaseNo", "InvNo", "Clone" ORDER BY "Start") AS rn
  FROM "MITELMAN"."PROD"."CYTOCONVERTED"
  WHERE "Type" = 'Loss' AND "ChrOrd" = 17 AND "Start" <= 7687490 AND "End" >= 7668421
), cc11_details AS (
  SELECT "RefNo", "CaseNo", "InvNo", "Clone", "ChrOrd", "Start", "End", ROW_NUMBER() OVER (PARTITION BY "RefNo", "CaseNo", "InvNo", "Clone" ORDER BY "Start") AS rn
  FROM "MITELMAN"."PROD"."CYTOCONVERTED"
  WHERE "Type" = 'Gain' AND "ChrOrd" = 11 AND "Start" <= 108369102 AND "End" >= 108223067
)
SELECT DISTINCT qc."RefNo", qc."CaseNo", qc."InvNo", qc."Clone", cc13."ChrOrd" AS "ChrOrd13", cc13."Start" AS "Start13", cc13."End" AS "End13", cc17."ChrOrd" AS "ChrOrd17", cc17."Start" AS "Start17", cc17."End" AS "End17", cc11."ChrOrd" AS "ChrOrd11", cc11."Start" AS "Start11", cc11."End" AS "End11", kc."CloneShort"
FROM qualifying_clones qc
INNER JOIN cc13_details cc13 ON qc."RefNo" = cc13."RefNo" AND qc."CaseNo" = cc13."CaseNo" AND qc."InvNo" = cc13."InvNo" AND qc."Clone" = cc13."Clone" AND cc13.rn = 1
INNER JOIN cc17_details cc17 ON qc."RefNo" = cc17."RefNo" AND qc."CaseNo" = cc17."CaseNo" AND qc."InvNo" = cc17."InvNo" AND qc."Clone" = cc17."Clone" AND cc17.rn = 1
INNER JOIN cc11_details cc11 ON qc."RefNo" = cc11."RefNo" AND qc."CaseNo" = cc11."CaseNo" AND qc."InvNo" = cc11."InvNo" AND qc."Clone" = cc11."Clone" AND cc11.rn = 1
LEFT JOIN "MITELMAN"."PROD"."KARYCLONE" kc ON qc."RefNo" = kc."RefNo" AND qc."CaseNo" = kc."CaseNo" AND qc."InvNo" = kc."InvNo" AND qc."Clone" = kc."CloneNo"