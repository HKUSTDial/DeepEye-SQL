SELECT 
    s."StyleName",
    COUNT(CASE WHEN p."PreferenceSeq" = 1 THEN 1 END) AS "1st_preference_count",
    COUNT(CASE WHEN p."PreferenceSeq" = 2 THEN 1 END) AS "2nd_preference_count",
    COUNT(CASE WHEN p."PreferenceSeq" = 3 THEN 1 END) AS "3rd_preference_count"
FROM "ENTERTAINMENTAGENCY"."ENTERTAINMENTAGENCY"."MUSICAL_STYLES" s
LEFT JOIN "ENTERTAINMENTAGENCY"."ENTERTAINMENTAGENCY"."MUSICAL_PREFERENCES" p
    ON s."StyleID" = p."StyleID" AND p."PreferenceSeq" IN (1, 2, 3)
GROUP BY s."StyleName"
ORDER BY s."StyleName"