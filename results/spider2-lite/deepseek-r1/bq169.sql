SELECT DISTINCT
  cc1.`RefNo`,
  cc1.`CaseNo`,
  cc1.`InvNo`,
  cc1.`Clone`,
  cc1.`ChrOrd` AS Chr13_ChrOrd,
  cc1.`Start` AS Chr13_Start,
  cc1.`End` AS Chr13_End,
  cc2.`ChrOrd` AS Chr17_ChrOrd,
  cc2.`Start` AS Chr17_Start,
  cc2.`End` AS Chr17_End,
  cc3.`ChrOrd` AS Chr11_ChrOrd,
  cc3.`Start` AS Chr11_Start,
  cc3.`End` AS Chr11_End,
  kc.`CloneShort`
FROM `mitelman-db.prod.CytoConverted` cc1
JOIN `mitelman-db.prod.CytoConverted` cc2
  ON cc1.`RefNo` = cc2.`RefNo`
  AND cc1.`CaseNo` = cc2.`CaseNo`
  AND cc1.`InvNo` = cc2.`InvNo`
  AND cc1.`Clone` = cc2.`Clone`
JOIN `mitelman-db.prod.CytoConverted` cc3
  ON cc1.`RefNo` = cc3.`RefNo`
  AND cc1.`CaseNo` = cc3.`CaseNo`
  AND cc1.`InvNo` = cc3.`InvNo`
  AND cc1.`Clone` = cc3.`Clone`
LEFT JOIN `mitelman-db.prod.KaryClone` kc
  ON cc1.`RefNo` = kc.`RefNo`
  AND cc1.`CaseNo` = kc.`CaseNo`
  AND cc1.`InvNo` = kc.`InvNo`
  AND cc1.`Clone` = kc.`CloneNo`
WHERE cc1.`Type` = 'Loss'
  AND cc1.`ChrOrd` = 13
  AND cc1.`Start` <= 48481890
  AND cc1.`End` >= 48303751
  AND cc2.`Type` = 'Loss'
  AND cc2.`ChrOrd` = 17
  AND cc2.`Start` <= 7687490
  AND cc2.`End` >= 7668421
  AND cc3.`Type` = 'Gain'
  AND cc3.`ChrOrd` = 11
  AND cc3.`Start` <= 108369102
  AND cc3.`End` >= 108223067