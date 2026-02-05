WITH counts AS (
    SELECT "L1_model", COUNT(*) AS "cnt"
    FROM "STACKING"."STACKING"."MODEL"
    GROUP BY "L1_model"
)
SELECT "L1_model", "cnt"
FROM counts
WHERE "cnt" = (SELECT MAX("cnt") FROM counts)