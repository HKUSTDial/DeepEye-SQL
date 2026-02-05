WITH RECURSIVE top_packaging AS (
  SELECT DISTINCT "packaging_id"
  FROM "ORACLE_SQL"."ORACLE_SQL"."PACKAGING_RELATIONS"
  WHERE "packaging_id" NOT IN (SELECT "contains_id" FROM "ORACLE_SQL"."ORACLE_SQL"."PACKAGING_RELATIONS")
),
cte AS (
  SELECT
    pr."packaging_id" AS "root_id",
    pr."contains_id" AS "child_id",
    pr."qty" AS "multiplier",
    ARRAY_CONSTRUCT(pr."packaging_id") AS "path"
  FROM "ORACLE_SQL"."ORACLE_SQL"."PACKAGING_RELATIONS" pr
  WHERE pr."packaging_id" IN (SELECT "packaging_id" FROM top_packaging)
  UNION ALL
  SELECT
    c."root_id",
    pr."contains_id" AS "child_id",
    c."multiplier" * pr."qty" AS "multiplier",
    ARRAY_APPEND(c."path", c."child_id") AS "path"
  FROM cte c
  INNER JOIN "ORACLE_SQL"."ORACLE_SQL"."PACKAGING_RELATIONS" pr
    ON c."child_id" = pr."packaging_id"
  WHERE NOT ARRAY_CONTAINS(pr."contains_id"::VARIANT, ARRAY_APPEND(c."path", c."child_id"))
)
SELECT AVG("total_quantity") AS "avg_total_quantity"
FROM (
  SELECT
    "root_id",
    SUM("multiplier") AS "total_quantity"
  FROM cte
  WHERE "child_id" NOT IN (SELECT "packaging_id" FROM "ORACLE_SQL"."ORACLE_SQL"."PACKAGING_RELATIONS")
  GROUP BY "root_id"
) leaf_totals