WITH style_scores AS (
    SELECT
        "StyleID",
        SUM(
            CASE "PreferenceSeq"
                WHEN 1 THEN 3
                WHEN 2 THEN 2
                WHEN 3 THEN 1
            END
        ) AS total_weighted_score
    FROM "ENTERTAINMENTAGENCY"."ENTERTAINMENTAGENCY"."MUSICAL_PREFERENCES"
    WHERE "PreferenceSeq" IN (1, 2, 3)
    GROUP BY "StyleID"
),
avg_score AS (
    SELECT AVG(total_weighted_score) AS avg_total_score
    FROM style_scores
)
SELECT
    ms."StyleName",
    ss."StyleID",
    ss.total_weighted_score,
    ABS(ss.total_weighted_score - a.avg_total_score) AS abs_diff
FROM style_scores ss
INNER JOIN "ENTERTAINMENTAGENCY"."ENTERTAINMENTAGENCY"."MUSICAL_STYLES" ms ON ss."StyleID" = ms."StyleID"
CROSS JOIN avg_score a
ORDER BY ms."StyleName"