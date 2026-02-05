WITH mitelman_freq AS (
    SELECT
        c."Chr" AS chromosome,
        cb."cytoband_name",
        COUNT(DISTINCT c."CaseNo") AS total_cases,
        SUM(CASE WHEN c."Type" = 'Amp' THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT c."CaseNo") AS freq_amp,
        SUM(CASE WHEN c."Type" = 'Gain' THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT c."CaseNo") AS freq_gain,
        SUM(CASE WHEN c."Type" = 'HomozygousDel' THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT c."CaseNo") AS freq_homodel,
        SUM(CASE WHEN c."Type" = 'HeterozygousDel' THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT c."CaseNo") AS freq_heterodel
    FROM
        "TCGA_MITELMAN"."PROD"."CYTOGEN" cyto
    INNER JOIN
        "TCGA_MITELMAN"."PROD"."CYTOCONVERTED" c
        ON cyto."RefNo" = c."RefNo" AND cyto."CaseNo" = c."CaseNo"
    INNER JOIN
        "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38" cb
        ON c."Chr" = cb."chromosome"
        AND c."Start" < cb."hg38_stop"
        AND c."End" > cb."hg38_start"
    WHERE
        cyto."Morph" = '3111'
        AND cyto."Topo" = '0401'
    GROUP BY
        c."Chr", cb."cytoband_name"
),
tcga_freq AS (
    SELECT
        cb."chromosome",
        cb."cytoband_name",
        COUNT(DISTINCT seg."case_barcode") AS total_cases,
        SUM(CASE WHEN seg."segment_mean" > 0.5849 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT seg."case_barcode") AS freq_amp,
        SUM(CASE WHEN seg."segment_mean" > 0.3219 AND seg."segment_mean" <= 0.5849 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT seg."case_barcode") AS freq_gain,
        SUM(CASE WHEN seg."segment_mean" < -1.3219 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT seg."case_barcode") AS freq_homodel,
        SUM(CASE WHEN seg."segment_mean" >= -1.3219 AND seg."segment_mean" < -0.3219 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT seg."case_barcode") AS freq_heterodel
    FROM
        "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_MASKED_HG38_GDC_R14" seg
    INNER JOIN
        "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38" cb
        ON 'chr' || seg."chromosome" = cb."chromosome"
        AND seg."start_pos" < cb."hg38_stop"
        AND seg."end_pos" > cb."hg38_start"
    WHERE
        seg."project_short_name" = 'TCGA-BRCA'
    GROUP BY
        cb."chromosome", cb."cytoband_name"
),
joined_data AS (
    SELECT
        m."chromosome",
        m."cytoband_name",
        m."freq_amp" AS m_amp,
        t."freq_amp" AS t_amp,
        m."freq_gain" AS m_gain,
        t."freq_gain" AS t_gain,
        m."freq_homodel" AS m_homodel,
        t."freq_homodel" AS t_homodel,
        m."freq_heterodel" AS m_heterodel,
        t."freq_heterodel" AS t_heterodel
    FROM
        mitelman_freq m
    INNER JOIN
        tcga_freq t
        ON m."chromosome" = t."chromosome"
        AND m."cytoband_name" = t."cytoband_name"
)
SELECT
    "chromosome",
    'amp' AS "aberration_type",
    CORR("m_amp", "t_amp") AS "correlation",
    CASE
        WHEN COUNT(*) >= 5 AND ABS(CORR("m_amp", "t_amp")) < 1 THEN
            2 * (1 - T_DISTRIBUTION(ABS(CORR("m_amp", "t_amp") * SQRT((COUNT(*) - 2) / (1 - POWER(CORR("m_amp", "t_amp"), 2)))), COUNT(*) - 2, TRUE))
        ELSE NULL
    END AS "p_value"
FROM
    joined_data
GROUP BY
    "chromosome"
HAVING
    COUNT(*) >= 5
    AND CORR("m_amp", "t_amp") IS NOT NULL

UNION ALL

SELECT
    "chromosome",
    'gain' AS "aberration_type",
    CORR("m_gain", "t_gain") AS "correlation",
    CASE
        WHEN COUNT(*) >= 5 AND ABS(CORR("m_gain", "t_gain")) < 1 THEN
            2 * (1 - T_DISTRIBUTION(ABS(CORR("m_gain", "t_gain") * SQRT((COUNT(*) - 2) / (1 - POWER(CORR("m_gain", "t_gain"), 2)))), COUNT(*) - 2, TRUE))
        ELSE NULL
    END AS "p_value"
FROM
    joined_data
GROUP BY
    "chromosome"
HAVING
    COUNT(*) >= 5
    AND CORR("m_gain", "t_gain") IS NOT NULL

UNION ALL

SELECT
    "chromosome",
    'loss' AS "aberration_type",
    CORR("m_heterodel", "t_heterodel") AS "correlation",
    CASE
        WHEN COUNT(*) >= 5 AND ABS(CORR("m_heterodel", "t_heterodel")) < 1 THEN
            2 * (1 - T_DISTRIBUTION(ABS(CORR("m_heterodel", "t_heterodel") * SQRT((COUNT(*) - 2) / (1 - POWER(CORR("m_heterodel", "t_heterodel"), 2)))), COUNT(*) - 2, TRUE))
        ELSE NULL
    END AS "p_value"
FROM
    joined_data
GROUP BY
    "chromosome"
HAVING
    COUNT(*) >= 5
    AND CORR("m_heterodel", "t_heterodel") IS NOT NULL

UNION ALL

SELECT
    "chromosome",
    'deletion' AS "aberration_type",
    CORR("m_homodel", "t_homodel") AS "correlation",
    CASE
        WHEN COUNT(*) >= 5 AND ABS(CORR("m_homodel", "t_homodel")) < 1 THEN
            2 * (1 - T_DISTRIBUTION(ABS(CORR("m_homodel", "t_homodel") * SQRT((COUNT(*) - 2) / (1 - POWER(CORR("m_homodel", "t_homodel"), 2)))), COUNT(*) - 2, TRUE))
        ELSE NULL
    END AS "p_value"
FROM
    joined_data
GROUP BY
    "chromosome"
HAVING
    COUNT(*) >= 5
    AND CORR("m_homodel", "t_homodel") IS NOT NULL
ORDER BY
    "chromosome", "aberration_type"