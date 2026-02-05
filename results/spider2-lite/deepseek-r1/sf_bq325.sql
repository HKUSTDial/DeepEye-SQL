WITH unified_associations AS (
    SELECT 
        "gene_id",
        "study_id",
        "pval" AS "pval"
    FROM "OPEN_TARGETS_GENETICS_2"."OPEN_TARGETS_GENETICS"."DISEASE_VARIANT_GENE"
    WHERE "pval" IS NOT NULL
    UNION ALL
    SELECT 
        "gene_id",
        "study_id",
        "pval" AS "pval"
    FROM "OPEN_TARGETS_GENETICS_2"."OPEN_TARGETS_GENETICS"."SA_MOLECULAR_TRAIT"
    WHERE "pval" IS NOT NULL
    UNION ALL
    SELECT 
        "gene_id",
        "study_id",
        "tag_pval" AS "pval"
    FROM "OPEN_TARGETS_GENETICS_2"."OPEN_TARGETS_GENETICS"."VARIANT_DISEASE_CREDSET"
    WHERE "tag_pval" IS NOT NULL
),
per_gene_study_min AS (
    SELECT 
        "gene_id",
        "study_id",
        MIN("pval") AS "min_pval"
    FROM unified_associations
    GROUP BY "gene_id", "study_id"
),
gene_best AS (
    SELECT 
        "gene_id",
        MIN("min_pval") AS "best_pval"
    FROM per_gene_study_min
    GROUP BY "gene_id"
)
SELECT 
    "gene_id",
    "best_pval"
FROM gene_best
ORDER BY "best_pval" ASC
LIMIT 10