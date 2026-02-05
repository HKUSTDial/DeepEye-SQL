WITH base AS (
    SELECT 
        c."state_code",
        c."state_code_name",
        pt."measurement_year",
        c."proportion_basis",
        c."condition_proportion_unadjusted",
        p."expansion_factor",
        p."adjustment_factor_for_the_subplot",
        p."adjustment_factor_for_the_macroplot",
        CASE 
            WHEN c."proportion_basis" = 'SUBP' AND p."adjustment_factor_for_the_subplot" > 0 
            THEN p."expansion_factor" * c."condition_proportion_unadjusted" * p."adjustment_factor_for_the_subplot"
            ELSE 0 
        END AS subplot_area,
        CASE 
            WHEN c."proportion_basis" = 'MACR' AND p."adjustment_factor_for_the_macroplot" > 0 
            THEN p."expansion_factor" * c."condition_proportion_unadjusted" * p."adjustment_factor_for_the_macroplot"
            ELSE 0 
        END AS macroplot_area
    FROM "USFS_FIA"."USFS_FIA"."CONDITION" c
    JOIN "USFS_FIA"."USFS_FIA"."PLOT_TREE" pt 
        ON c."plot_sequence_number" = pt."plot_sequence_number"
    JOIN "USFS_FIA"."USFS_FIA"."POPULATION" p 
        ON c."state_code" = p."state_code" 
        AND c."inventory_year" = p."inventory_year" 
        AND c."plot_sequence_number" = p."plot_sequence_number"
    WHERE c."condition_status_code" = 1
        AND pt."measurement_year" IN (2015, 2016, 2017)
        AND p."evaluation_type" = 'EXPCURR'
),
subplot_avg AS (
    SELECT 
        "measurement_year",
        "state_code",
        "state_code_name",
        AVG(subplot_area) AS avg_subplot_size
    FROM base
    WHERE "proportion_basis" = 'SUBP'
    GROUP BY "measurement_year", "state_code", "state_code_name"
),
macroplot_avg AS (
    SELECT 
        "measurement_year",
        "state_code",
        "state_code_name",
        AVG(macroplot_area) AS avg_macroplot_size
    FROM base
    WHERE "proportion_basis" = 'MACR'
    GROUP BY "measurement_year", "state_code", "state_code_name"
),
subplot_max AS (
    SELECT 
        "measurement_year",
        "state_code",
        "state_code_name",
        avg_subplot_size,
        RANK() OVER (PARTITION BY "measurement_year" ORDER BY avg_subplot_size DESC) AS rnk
    FROM subplot_avg
),
macroplot_max AS (
    SELECT 
        "measurement_year",
        "state_code",
        "state_code_name",
        avg_macroplot_size,
        RANK() OVER (PARTITION BY "measurement_year" ORDER BY avg_macroplot_size DESC) AS rnk
    FROM macroplot_avg
)
SELECT 
    'subplot' AS plot_type,
    "measurement_year" AS year,
    "state_code_name" AS state,
    avg_subplot_size AS average_size
FROM subplot_max
WHERE rnk = 1
UNION ALL
SELECT 
    'macroplot' AS plot_type,
    "measurement_year" AS year,
    "state_code_name" AS state,
    avg_macroplot_size AS average_size
FROM macroplot_max
WHERE rnk = 1
ORDER BY plot_type, year