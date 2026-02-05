WITH unified_edges AS (
  SELECT
    "sender" AS source_addr,
    "to_addr" AS dest_addr,
    "id" AS tx_id,
    "block_timestamp" AS block_timestamp,
    "block_number" AS block_number,
    "accepted" AS accepted,
    "success" AS success,
    'transaction' AS edge_type
  FROM "CRYPTO"."CRYPTO_ZILLIQA"."TRANSACTIONS"
  WHERE "accepted" = TRUE AND "success" = TRUE
  UNION ALL
  SELECT
    "addr" AS source_addr,
    "recipient" AS dest_addr,
    "transaction_id" AS tx_id,
    "block_timestamp" AS block_timestamp,
    "block_number" AS block_number,
    "accepted" AS accepted,
    NULL AS success,
    'transition' AS edge_type
  FROM "CRYPTO"."CRYPTO_ZILLIQA"."TRANSITIONS"
  WHERE "accepted" = TRUE
),
edges_from_source AS (
  SELECT *
  FROM unified_edges
  WHERE source_addr = 'zil1jrpjd8pjuv50cfkfr7eu6yrm3rn5u8rulqhqpz'
),
edges_to_dest AS (
  SELECT *
  FROM unified_edges
  WHERE dest_addr = 'zil19nmxkh020jnequql9kvqkf3pkwm0j0spqtd26e'
),
intermediate_candidates AS (
  SELECT DISTINCT e1.dest_addr AS intermediate
  FROM edges_from_source e1
  INNER JOIN edges_to_dest e2 ON e1.dest_addr = e2.source_addr
  WHERE e1.block_timestamp < e2.block_timestamp
),
intermediate_outcount AS (
  SELECT ic.intermediate, COUNT(ue.*) AS out_count
  FROM intermediate_candidates ic
  JOIN unified_edges ue ON ic.intermediate = ue.source_addr
  GROUP BY ic.intermediate
  HAVING COUNT(*) <= 50
)
SELECT
  e1.source_addr || ' --(tx ' || SUBSTR(e1.tx_id, 1, 5) || '..)--> ' || e1.dest_addr || ' --(tx ' || SUBSTR(e2.tx_id, 1, 5) || '..)--> ' || e2.dest_addr AS path
FROM edges_from_source e1
INNER JOIN edges_to_dest e2 ON e1.dest_addr = e2.source_addr
INNER JOIN intermediate_outcount ioc ON e1.dest_addr = ioc.intermediate
WHERE e1.block_timestamp < e2.block_timestamp
ORDER BY e1.block_timestamp, e2.block_timestamp;