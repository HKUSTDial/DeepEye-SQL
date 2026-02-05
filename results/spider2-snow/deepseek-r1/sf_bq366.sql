WITH base AS (
    SELECT o."period", v."object_id", flt.value:"description"::TEXT AS label
    FROM "THE_MET"."THE_MET"."OBJECTS" o
    JOIN "THE_MET"."THE_MET"."VISION_API_DATA" v ON o."object_id" = v."object_id"
    , LATERAL FLATTEN(INPUT => v."labelAnnotations") flt
    WHERE o."period" IS NOT NULL
), label_totals AS (
    SELECT label, COUNT(DISTINCT "object_id") AS total_artworks
    FROM base
    GROUP BY label
    HAVING total_artworks >= 500
), period_label_counts AS (
    SELECT "period", label, COUNT(DISTINCT "object_id") AS count_artworks
    FROM base
    GROUP BY "period", label
), filtered_period_labels AS (
    SELECT plc."period", plc.label, plc.count_artworks
    FROM period_label_counts plc
    INNER JOIN label_totals lt ON plc.label = lt.label
), ranked AS (
    SELECT "period", label, count_artworks, ROW_NUMBER() OVER (PARTITION BY "period" ORDER BY count_artworks DESC) AS rn
    FROM filtered_period_labels
)
SELECT "period", label, count_artworks AS associated_count
FROM ranked
WHERE rn <= 3
ORDER BY "period", rn