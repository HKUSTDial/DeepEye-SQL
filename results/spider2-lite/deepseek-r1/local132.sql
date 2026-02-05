WITH `EntertainerTotal` AS (
    SELECT `EntertainerID`, COUNT(*) AS `TotalStyles`
    FROM `Entertainer_Styles`
    GROUP BY `EntertainerID`
    HAVING COUNT(*) <= 3
),
`EntertainerFirstTwo` AS (
    SELECT es.`EntertainerID`,
           MAX(CASE WHEN es.`StyleStrength` = 1 THEN es.`StyleID` END) AS `Style1`,
           MAX(CASE WHEN es.`StyleStrength` = 2 THEN es.`StyleID` END) AS `Style2`
    FROM `Entertainer_Styles` es
    INNER JOIN `EntertainerTotal` et ON es.`EntertainerID` = et.`EntertainerID`
    WHERE es.`StyleStrength` IN (1, 2)
    GROUP BY es.`EntertainerID`
    HAVING COUNT(*) = 2
),
`CustomerTotal` AS (
    SELECT `CustomerID`, COUNT(*) AS `TotalPrefs`
    FROM `Musical_Preferences`
    GROUP BY `CustomerID`
    HAVING COUNT(*) <= 3
),
`CustomerFirstTwo` AS (
    SELECT mp.`CustomerID`,
           MAX(CASE WHEN mp.`PreferenceSeq` = 1 THEN mp.`StyleID` END) AS `Pref1`,
           MAX(CASE WHEN mp.`PreferenceSeq` = 2 THEN mp.`StyleID` END) AS `Pref2`
    FROM `Musical_Preferences` mp
    INNER JOIN `CustomerTotal` ct ON mp.`CustomerID` = ct.`CustomerID`
    WHERE mp.`PreferenceSeq` IN (1, 2)
    GROUP BY mp.`CustomerID`
    HAVING COUNT(*) = 2
)
SELECT DISTINCT e.`EntStageName`, c.`CustLastName`
FROM `EntertainerFirstTwo` ef
JOIN `CustomerFirstTwo` cf ON (ef.`Style1` = cf.`Pref1` AND ef.`Style2` = cf.`Pref2`) OR (ef.`Style1` = cf.`Pref2` AND ef.`Style2` = cf.`Pref1`)
INNER JOIN `Entertainers` e ON ef.`EntertainerID` = e.`EntertainerID`
INNER JOIN `Customers` c ON cf.`CustomerID` = c.`CustomerID`