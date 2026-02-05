WITH WeightedScores AS (
    SELECT 
        `StyleID`,
        SUM(CASE `PreferenceSeq`
            WHEN 1 THEN 3
            WHEN 2 THEN 2
            WHEN 3 THEN 1
            ELSE 0
        END) AS `weighted_score`
    FROM `Musical_Preferences`
    GROUP BY `StyleID`
),
AverageScore AS (
    SELECT AVG(`weighted_score`) AS `avg_score`
    FROM WeightedScores
)
SELECT 
    ms.`StyleID`,
    ms.`StyleName`,
    ws.`weighted_score`,
    ABS(ws.`weighted_score` - a.`avg_score`) AS `score_difference`
FROM WeightedScores ws
CROSS JOIN AverageScore a
JOIN `Musical_Styles` ms ON ws.`StyleID` = ms.`StyleID`
ORDER BY ms.`StyleID`