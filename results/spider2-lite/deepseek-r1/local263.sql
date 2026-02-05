WITH step_scores AS (
    SELECT 
        name,
        version,
        step,
        MAX(CASE WHEN model != 'Stack' THEN test_score END) AS max_non_stack,
        MAX(CASE WHEN model = 'Stack' THEN test_score END) AS stack_score
    FROM model_score
    GROUP BY name, version, step
    HAVING stack_score IS NOT NULL AND max_non_stack IS NOT NULL
),
model_status AS (
    SELECT 
        name,
        version,
        CASE 
            WHEN SUM(CASE WHEN max_non_stack < stack_score THEN 1 ELSE 0 END) > 0 THEN 'strong'
            WHEN SUM(CASE WHEN max_non_stack = stack_score THEN 1 ELSE 0 END) > 0 THEN 'soft'
            ELSE NULL
        END AS status
    FROM step_scores
    GROUP BY name, version
),
model_l1 AS (
    SELECT name, version, MIN(L1_model) AS L1_model
    FROM model
    GROUP BY name, version
),
joined AS (
    SELECT 
        m.L1_model,
        s.status
    FROM model_l1 m
    INNER JOIN model_status s ON m.name = s.name AND m.version = s.version
    WHERE s.status IS NOT NULL
),
counts AS (
    SELECT 
        status,
        L1_model,
        COUNT(*) AS count
    FROM joined
    GROUP BY status, L1_model
),
ranked AS (
    SELECT 
        status,
        L1_model,
        count,
        RANK() OVER (PARTITION BY status ORDER BY count DESC) AS rnk
    FROM counts
)
SELECT 
    status,
    L1_model,
    count
FROM ranked
WHERE rnk = 1
ORDER BY status;