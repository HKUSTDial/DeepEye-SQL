WITH top_level_packaging AS (
  SELECT p."id", p."name"
  FROM "ORACLE_SQL"."ORACLE_SQL"."PACKAGING" p
  WHERE p."id" NOT IN (
    SELECT DISTINCT pr."contains_id"
    FROM "ORACLE_SQL"."ORACLE_SQL"."PACKAGING_RELATIONS" pr
    WHERE pr."contains_id" IS NOT NULL
  )
),
recursive_hierarchy AS (
  SELECT tlp."id" AS top_packaging_id, tlp."id" AS node_id, 1.0 AS cumulative_quantity, CAST('PACKAGING' AS VARCHAR) AS node_type
  FROM top_level_packaging tlp
  UNION ALL
  SELECT rh.top_packaging_id, pr."contains_id" AS node_id, rh.cumulative_quantity * pr."qty" AS cumulative_quantity, CASE WHEN EXISTS (SELECT 1 FROM "ORACLE_SQL"."ORACLE_SQL"."PACKAGING" p WHERE p."id" = pr."contains_id") THEN 'PACKAGING' ELSE 'PRODUCT' END AS node_type
  FROM recursive_hierarchy rh
  INNER JOIN "ORACLE_SQL"."ORACLE_SQL"."PACKAGING_RELATIONS" pr ON rh.node_id = pr."packaging_id"
  WHERE rh.node_type = 'PACKAGING'
),
product_totals AS (
  SELECT top_packaging_id, node_id AS product_id, SUM(cumulative_quantity) AS total_quantity
  FROM recursive_hierarchy
  WHERE node_type = 'PRODUCT'
  GROUP BY top_packaging_id, product_id
  HAVING SUM(cumulative_quantity) > 500
)
SELECT p."name" AS packaging_name, prod."name" AS product_name
FROM product_totals pt
JOIN "ORACLE_SQL"."ORACLE_SQL"."PACKAGING" p ON pt.top_packaging_id = p."id"
JOIN "ORACLE_SQL"."ORACLE_SQL"."PRODUCTS" prod ON pt.product_id = prod."id"
ORDER BY p."name", prod."name"