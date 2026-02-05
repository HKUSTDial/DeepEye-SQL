WITH ALL_GENE_PVALUES AS (
    SELECT 
        "gene_id",
        "study_id",
        "tag_pval" AS "p_value"
    FROM "OPEN_TARGETS_GENETICS_2"."OPEN_TARGETS_GENETICS"."VARIANT_DISEASE_CREDSET"
    WHERE "tag_pval" IS NOT NULL
    
    UNION ALL
    
    SELECT 
        "gene_id",
        "study_id", 
        "pval" AS "p_value"
    FROM "OPEN_TARGETS_GENETICS_2"."OPEN_TARGETS_GENETICS"."SA_MOLECULAR_TRAIT"
    WHERE "pval" IS NOT NULL
    
    UNION ALL
    
    SELECT 
        "gene_id",
        "study_id",
        LEAST(
            COALESCE("pval", 1.0),
            COALESCE("qtl_pval", 1.0)
        ) AS "p_value"
    FROM "OPEN_TARGETS_GENETICS_2"."OPEN_TARGETS_GENETICS"."DISEASE_VARIANT_GENE"
    WHERE "pval" IS NOT NULL OR "qtl_pval" IS NOT NULL
),
MIN_PVALUE_PER_STUDY AS (
    SELECT 
        "gene_id",
        "study_id",
        MIN("p_value") AS "min_p_value"
    FROM ALL_GENE_PVALUES
    GROUP BY "gene_id", "study_id"
),
OVERALL_GENE_RANKING AS (
    SELECT 
        "gene_id",
        MIN("min_p_value") AS "overall_min_p_value"
    FROM MIN_PVALUE_PER_STUDY
    GROUP BY "gene_id"
)
SELECT 
    g."gene_id",
    g."gene_name",
    ogr."overall_min_p_value"
FROM OVERALL_GENE_RANKING ogr
JOIN "OPEN_TARGETS_GENETICS_2"."OPEN_TARGETS_GENETICS"."GENES" g
    ON ogr."gene_id" = g."gene_id"
ORDER BY ogr."overall_min_p_value" ASC
LIMIT 10