WITH solution_counts AS (
    SELECT "name", COUNT(*) AS solution_count
    FROM "STACKING"."STACKING"."SOLUTION"
    GROUP BY "name"
), model_score_groups AS (
    SELECT "name", "step", "version",
        MAX(CASE WHEN "model" != 'Stack' THEN "test_score" END) AS max_non_stack,
        MAX(CASE WHEN "model" = 'Stack' THEN "test_score" END) AS stack_score
    FROM "STACKING"."STACKING"."MODEL_SCORE"
    WHERE "step" IN (1, 2, 3)
    GROUP BY "name", "step", "version"
    HAVING max_non_stack IS NOT NULL AND stack_score IS NOT NULL
), condition_holds AS (
    SELECT "name", "step", "version"
    FROM model_score_groups
    WHERE max_non_stack < stack_score
), condition_counts AS (
    SELECT "name", COUNT(*) AS condition_count
    FROM condition_holds
    GROUP BY "name"
)
SELECT c."name"
FROM condition_counts c
LEFT JOIN solution_counts s ON c."name" = s."name"
WHERE c.condition_count > COALESCE(s.solution_count, 0)
ORDER BY c."name"