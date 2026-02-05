WITH step_scores AS (
    SELECT 
        "name",
        "version",
        "step",
        MAX(CASE WHEN "model" = 'Stack' THEN "test_score" END) AS stack_score,
        MAX(CASE WHEN "model" != 'Stack' THEN "test_score" END) AS max_non_stack_score
    FROM "STACKING"."STACKING"."MODEL_SCORE"
    GROUP BY "name", "version", "step"
    HAVING stack_score IS NOT NULL AND max_non_stack_score IS NOT NULL
),
step_indicators AS (
    SELECT 
        "name",
        "version",
        "step",
        stack_score,
        max_non_stack_score,
        CASE WHEN max_non_stack_score < stack_score THEN 1 ELSE 0 END AS is_strong_step,
        CASE WHEN max_non_stack_score = stack_score THEN 1 ELSE 0 END AS is_soft_step
    FROM step_scores
),
model_agg AS (
    SELECT 
        "name",
        "version",
        MAX(is_strong_step) AS has_strong,
        MAX(is_soft_step) AS has_soft
    FROM step_indicators
    GROUP BY "name", "version"
),
model_status AS (
    SELECT 
        "name",
        "version",
        CASE 
            WHEN has_strong = 1 THEN 'strong'
            WHEN has_soft = 1 THEN 'soft'
            ELSE NULL
        END AS status
    FROM model_agg
    WHERE has_strong = 1 OR has_soft = 1
),
distinct_l1 AS (
    SELECT DISTINCT "name", "version", "L1_model"
    FROM "STACKING"."STACKING"."MODEL"
),
model_with_l1 AS (
    SELECT 
        m."name",
        m."version",
        m.status,
        d."L1_model"
    FROM model_status m
    JOIN distinct_l1 d ON m."name" = d."name" AND m."version" = d."version"
),
counts AS (
    SELECT 
        status,
        "L1_model",
        COUNT(*) AS occurrence_count
    FROM model_with_l1
    GROUP BY status, "L1_model"
),
ranked AS (
    SELECT 
        status,
        "L1_model",
        occurrence_count,
        RANK() OVER (PARTITION BY status ORDER BY occurrence_count DESC) AS rnk
    FROM counts
)
SELECT 
    status,
    "L1_model",
    occurrence_count
FROM ranked
WHERE rnk = 1
ORDER BY status